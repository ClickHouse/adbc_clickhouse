//! Client-side TLS configuration for the HTTP transport.
//!
//! When the crate is built with a crypto-provider feature (`rustls-tls-aws-lc` or
//! `rustls-tls-ring`, which enable the internal `tls-mtls` feature), the driver can verify the
//! ClickHouse server against a caller-supplied PEM root bundle (replacing the built-in roots)
//! and/or present a client identity (PEM certificate chain + private key) for mutual TLS.
//!
//! The transport is assembled into a custom [`rustls::ClientConfig`] that is handed to the
//! underlying `clickhouse` client through [`clickhouse::Client::with_http_client`], which accepts
//! any `hyper-util` legacy client whose connector is a [`hyper_rustls::HttpsConnector`].

use std::io::BufReader;

use adbc_core::error::{Error, Status};

use crate::options::{MTLS_CERT_CHAIN, MTLS_PRIVATE_KEY, TLS_ROOT_CERTS};

/// The TLS trust and identity options for the HTTP transport.
///
/// Holds the raw PEM strings set through the driver options. Each PEM value is validated as soon
/// as it is set; whether a client identity is fully specified (both halves) is only checked when
/// the transport is built, so a caller may set the two halves across separate option-setting calls.
#[derive(Clone, Default)]
pub(crate) struct TlsOptions {
    /// PEM root certificates that *replace* the built-in roots when set.
    pub(crate) root_certs: Option<String>,
    /// PEM client certificate chain for mutual TLS.
    pub(crate) client_cert_chain: Option<String>,
    /// PEM client private key for mutual TLS.
    pub(crate) client_private_key: Option<String>,
}

// The private key is a secret, so `Debug` reports only whether it is set.
impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field("root_certs", &self.root_certs)
            .field("client_cert_chain", &self.client_cert_chain)
            .field(
                "client_private_key",
                &self.client_private_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Whether `key` is one of this module's TLS options.
pub(crate) fn is_tls_option(key: &str) -> bool {
    matches!(key, TLS_ROOT_CERTS | MTLS_CERT_CHAIN | MTLS_PRIVATE_KEY)
}

impl TlsOptions {
    pub(crate) fn is_empty(&self) -> bool {
        self.root_certs.is_none()
            && self.client_cert_chain.is_none()
            && self.client_private_key.is_none()
    }

    /// Validate and store a single option value.
    ///
    /// Malformed PEM is refused with [`Status::InvalidArguments`] immediately. The mutual-TLS
    /// pairing rule (certificate chain without key, or key without chain) is enforced later, at
    /// transport-build time, via [`Self::check_identity`]. Without a crypto-provider feature this
    /// refuses loudly rather than silently ignoring the option.
    pub(crate) fn try_set(&mut self, key: &str, value: String) -> Result<(), Error> {
        #[cfg(feature = "tls-mtls")]
        {
            match key {
                TLS_ROOT_CERTS => {
                    ensure_certificates(&value, key)?;
                    self.root_certs = Some(value);
                }
                MTLS_CERT_CHAIN => {
                    ensure_certificates(&value, key)?;
                    self.client_cert_chain = Some(value);
                }
                MTLS_PRIVATE_KEY => {
                    parse_private_key(&value, key)?;
                    self.client_private_key = Some(value);
                }
                other => return Err(unknown_option(other)),
            }
            Ok(())
        }

        #[cfg(not(feature = "tls-mtls"))]
        {
            let _ = (&key, &value);
            Err(Error::with_message_and_status(
                format!(
                    "option {key:?} requires this crate to be built with a \
                     `rustls-tls-aws-lc` or `rustls-tls-ring` feature"
                ),
                Status::InvalidArguments,
            ))
        }
    }

    /// Refuse a client identity specified with only one half (chain without key, or key without
    /// chain).
    pub(crate) fn check_identity(&self) -> Result<(), Error> {
        if self.client_cert_chain.is_some() != self.client_private_key.is_some() {
            return Err(Error::with_message_and_status(
                format!(
                    "client identity must set both {MTLS_CERT_CHAIN:?} and {MTLS_PRIVATE_KEY:?}; \
                     got certificate chain: {}, private key: {}",
                    self.client_cert_chain.is_some(),
                    self.client_private_key.is_some(),
                ),
                Status::InvalidArguments,
            ));
        }
        Ok(())
    }

    /// Read back an option's stored value.
    ///
    /// The private key is treated as a secret and its read-back is refused with
    /// [`Status::NotFound`] (mirroring the driver's handling of other sensitive options).
    pub(crate) fn read_back(&self, key: &str) -> Result<String, Error> {
        match key {
            MTLS_PRIVATE_KEY => Err(Error::with_message_and_status(
                "read-back of the private key has been omitted to avoid leaking a secret",
                Status::NotFound,
            )),
            TLS_ROOT_CERTS => Ok(self.root_certs.clone().unwrap_or_default()),
            MTLS_CERT_CHAIN => Ok(self.client_cert_chain.clone().unwrap_or_default()),
            other => Err(unknown_option(other)),
        }
    }

    /// Build the `hyper-rustls` HTTPS connector carrying this configuration.
    ///
    /// Only called when `!self.is_empty()`, i.e. the caller wants to customize the transport.
    #[cfg(feature = "tls-mtls")]
    pub(crate) fn build_https_connector(
        &self,
    ) -> Result<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Error,
    > {
        use hyper_rustls::ConfigBuilderExt;
        use hyper_rustls::HttpsConnectorBuilder;
        use hyper_util::client::legacy::connect::HttpConnector;
        use rustls::{ClientConfig, RootCertStore};

        self.check_identity()?;

        // Mirror the `clickhouse` crate: the crypto provider follows the enabled feature.
        #[cfg(feature = "rustls-tls-aws-lc")]
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        #[cfg(all(feature = "rustls-tls-ring", not(feature = "rustls-tls-aws-lc")))]
        let provider = rustls::crypto::ring::default_provider();

        let builder = ClientConfig::builder_with_provider(std::sync::Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(transport_error)?;

        // A caller-supplied bundle *replaces* (rather than extends) the built-in root store.
        let builder = if let Some(pem) = &self.root_certs {
            let mut store = RootCertStore::empty();
            store.add_parsable_certificates(parse_certificates(pem, TLS_ROOT_CERTS)?);
            builder.with_root_certificates(store)
        } else {
            #[cfg(feature = "rustls-tls-native-roots")]
            {
                builder.with_native_roots().map_err(transport_error)?
            }
            #[cfg(all(
                feature = "rustls-tls-webpki-roots",
                not(feature = "rustls-tls-native-roots")
            ))]
            {
                builder.with_webpki_roots()
            }
        };

        let config = if let (Some(certs), Some(key)) = (
            self.client_cert_chain.as_deref(),
            self.client_private_key.as_deref(),
        ) {
            builder
                .with_client_auth_cert(
                    parse_certificates(certs, MTLS_CERT_CHAIN)?,
                    parse_private_key(key, MTLS_PRIVATE_KEY)?,
                )
                .map_err(|e| {
                    invalid_arguments(format!("error configuring client certificate: {e}"))
                })?
        } else {
            builder.with_no_client_auth()
        };

        let http = HttpConnector::new();
        Ok(HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_or_http()
            .enable_http1()
            .wrap_connector(http))
    }
}

#[cfg(feature = "tls-mtls")]
fn parse_certificates(
    pem: &str,
    key: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
    rustls_pemfile::certs(&mut BufReader::new(pem.as_bytes()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| invalid_arguments(format!("malformed PEM in option {key:?}: {e}")))
}

#[cfg(feature = "tls-mtls")]
fn ensure_certificates(pem: &str, key: &str) -> Result<(), Error> {
    let certs = parse_certificates(pem, key)?;
    if certs.is_empty() {
        return Err(invalid_arguments(format!(
            "no certificates found in option {key:?}"
        )));
    }
    Ok(())
}

#[cfg(feature = "tls-mtls")]
fn parse_private_key(
    pem: &str,
    key: &str,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut BufReader::new(pem.as_bytes()))
        .map_err(|e| invalid_arguments(format!("malformed PEM in option {key:?}: {e}")))?
        .ok_or_else(|| invalid_arguments(format!("no private key found in option {key:?}")))
}

fn invalid_arguments(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

fn transport_error(e: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(
        format!("error building TLS configuration: {e}"),
        Status::Internal,
    )
}

fn unknown_option(key: &str) -> Error {
    Error::with_message_and_status(
        format!("unknown TLS option {key:?}"),
        Status::InvalidArguments,
    )
}

#[cfg(all(test, feature = "tls-mtls"))]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    const CA_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC1DCCAbygAwIBAgIJAN6l6UQNGiZvMA0GCSqGSIb3DQEBCwUAMBcxFTATBgNV
BAMMDGFkYmMtdGVzdC1jYTAeFw0yNjEwMDIxMTU1NDRaFw0zNjA5MjkxMTU1NDRa
MBcxFTATBgNVBAMMDGFkYmMtdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEP
ADCCAQoCggEBANiRjQ301ic4oFdnLqLO/h9tOUSMv6pnOnnRQaS0kF3nI3DxbWwd
UwA2OHMgfwr2hyDR0RQ8kApI2yqNx5L00vXTd+1NYfc+gErGzQQNaNatFa7qqm3r
NVAmlESRKPb9qhahUY/6T1fYcA7S91kHd6r0JDHrfYuVfmGuW+YA5m5qmQXonrhb
5yzKZGs+0R7Rnz7yn2smpFhMgKlqXg47aUpHm2YMQN/GphNCr4nOMDl0s6k675Zv
7MAk60jV+NmD46zLbWg8E2idjlnF3eJVeoIdQUpruL+M6rhsnLNEZ/2j2sqekpKJ
UO3mN8U3qHp/kI0oX8U2Y7YsnFy4RIrtdfkCAwEAAaMjMCEwDwYDVR0TAQH/BAUw
AwEB/zAOBgNVHQ8BAf8EBAMCAQYwDQYJKoZIhvcNAQELBQADggEBABPtldaCqF0v
x2ddEKh/erNzoPd8FU6MLRJ7bZdst8/FdcirzheKAohY0IlShBkWT0uRV78YfjuR
P9RpgWlZIaJXx8/H0SxXSJi3mxLbfRq3sAHoN70fnZOUaJrNCMk1P1s5ludcbvu0
k/wIJvfa7U6EUK3gv18Vn/w99XtpDbk7gzgZXL7+jHjGL3iyxNG5G2QKmLyCDhoD
XZ7jVyZrco9+kOaR+NKVJOkFA7yja/jp9nhNfE28pO4bYdkhW43sEZM5SaWeV3vH
HBrp40dYSiXyHQRNhvjh+lATV+VRGQX+ZgZH/i5PUSyiokFBY9duhcMmq4uQUUA8
BAVnu77lGtc=
-----END CERTIFICATE-----"#;
    const CLIENT_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC3TCCAcWgAwIBAgIJAKO/xlCxuIUKMA0GCSqGSIb3DQEBCwUAMBcxFTATBgNV
BAMMDGFkYmMtdGVzdC1jYTAeFw0yNjEwMDIxMTU1NDRaFw0zNjA5MjkxMTU1NDRa
MBExDzANBgNVBAMMBmNsaWVudDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAMXX9+eo1kMLwibbe4zErkQoXGQtFcgqvIvjLTpHgzk0i8ffdKSdpgfx45hz
5Y6IkssHOy5fW1T/RSs6WDCekj1f4C/c6f/6gAF7FV2sHSI7Kzst2QhAYobyHVf2
DOYYfCL5r7jdddguwpLnPyE0LBTFXtZIE8b0rZmpyyCuR0Rqz+JPRqxibnTjnzAm
TdV14Dn/t/d35XpEvuGh9V1kpF0n45lz/jL1Wa/CPTyzX5L8AQAuvIKHSy7fqVs4
lmLc5rQLNwpw2hRAM1hFn+z30QMHxPoBGXswhBHNJuf+60LtU263cm6UV6th/JmL
SnCNa/iNnaxNItm1NhfhKOqBx9kCAwEAAaMyMDAwDAYDVR0TAQH/BAIwADATBgNV
HSUEDDAKBggrBgEFBQcDAjALBgNVHQ8EBAMCBaAwDQYJKoZIhvcNAQELBQADggEB
ALeAdzcWSrFd4EroPu+LJKIqTYWCRcls2wYk6d6g1E5HCx48bonFpOt6WiVjNsko
rggUV8NwbO3F5fPIbu/z9OU+umXNPcBAL4HVMILB5gbUkItDZ591hWjcZYZ4IkVc
nCbVG2LOOWJKebED0urhtCMTj/1KWiJRwMJd3LC08CPhPRjWgXfj8+XF49dfeM74
dfKncbqZTeZgweYpf46nW5J0sMADgCx43JY7mBG61uGitReG1ROII+987wFUAFK4
Rm9QLDqbJXM9G8IpZMLt0lMaFW5t4YJATzK9VzpgsQIpJjqQ6yFGAP+eKZLlNPHT
3dpoWDnAH7z6djCo2eocDzM=
-----END CERTIFICATE-----"#;
    const CLIENT_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAxdf356jWQwvCJtt7jMSuRChcZC0VyCq8i+MtOkeDOTSLx990
pJ2mB/HjmHPljoiSywc7Ll9bVP9FKzpYMJ6SPV/gL9zp//qAAXsVXawdIjsrOy3Z
CEBihvIdV/YM5hh8IvmvuN112C7Ckuc/ITQsFMVe1kgTxvStmanLIK5HRGrP4k9G
rGJudOOfMCZN1XXgOf+393flekS+4aH1XWSkXSfjmXP+MvVZr8I9PLNfkvwBAC68
godLLt+pWziWYtzmtAs3CnDaFEAzWEWf7PfRAwfE+gEZezCEEc0m5/7rQu1Tbrdy
bpRXq2H8mYtKcI1r+I2drE0i2bU2F+Eo6oHH2QIDAQABAoIBABKSW2sbpkFU8TzE
jayBKfqr70BYGxtXJ6vZ/3zhCahtDlIM3940DkngowTEnF/s4hq86wlHWkN4Uv3j
1DN8Ly4jud/EVRL6aNm6vB628WUtOLmtbBoiMR2JzMddCYjv83c1CRHJeC569Tjl
DEAJrbmlbWmgCns6rLEoBzFiKr55PfqQxcleJsVObSj34zSTQR+lD5aqgbOZYVIj
seUKQTcTArKf3X/r1GPxBJl+N31CwvBq4Rb+mUi3zOD93z4C5mrX0iTsHGk+g0Cz
FWqqM0Pwuyf3dKFeA1HAhm+tKsQHICzomlilxzz98bGKnPAqZGoGxhnz4uaGE7WI
rAMlEIECgYEA8typ0h9DWTp5i4C7bGAVjTv9VMBZqTrKxLI45nnV4vofmuuu8x0w
3sOjQHFj2mOPSdTh0hFzsR+VyQM5rd8xiMqaboFeTcUkM+pX8XmWjXEAEGtuuqiv
cADpg7ZmY72Y9OqjGdPu6/apmLiT5Lm0Lb3aV9j5YvusFimb6I5sxxUCgYEA0Ivb
XtLd1Cs3tNV9rqwJMz8AFtN+bZiQvh3kEDCeJDPq6jvPsODH9nAOpyJpx0mhTixj
0rpT1wJCpw1Pyw67fqFGTCqZ+5uN/svgHGa+5RqvhkE69Zd4j5Tj9mkKR43oAHhl
RZOmbJbgZJDkJwNxhUk3GIVYioDIcwFlenVWbrUCgYEAqtSC89Wf1xzlsDYuH+56
Wm1PJhVjPEGprbA7jAs5Yz5TivKrjkEDRj7RD5xzA4Qyn1mKOuyht36jKtEE+oPb
4P+6n26KeiPo8ket8qJ3Pyi0Rdh7+/p55UwO9eIQpaoQ9H4zmhBUqFBKPa3gAeyK
i/5WnqJ10aV8B97e1zeP1n0CgYEAyOMQDAWf/bMr1BwbF1lM+3pigZp1HOlr9Fci
y52pmEIStlOrOz0sIe3Lj6AMuRXqNj14YxNtk53vyq99/6JPZinAZ7pZ3g7390s4
tOMcW+Ea136aZtzkG5tXBJQmfMrrgxh2pVEXbV7hBL61qAcfIT9nvU0l/zUS4gWk
VRQWkEkCgYEAitkEz6TO4Tl+38y/FM/vMB/z6fpxtIGyYbx92Mtllcxi1mSgW6K4
A8/soZvMNWlT6HvX2o+D8hxV+vrT07+ezjl8rYfQysHcGcwXyfGVo67zVs1ZOm+3
O6N3BhqxCUTBtZJDOj2gZP5b1REU3YrRIbCNPX8Z29s1ZSXCRCenGUc=
-----END RSA PRIVATE KEY-----"#;

    #[test]
    fn tls_option_names_use_repo_style() {
        assert_eq!(TLS_ROOT_CERTS, "clickhouse.client.tls_root_certs");
        assert_eq!(MTLS_CERT_CHAIN, "clickhouse.client.mtls_cert_chain");
        assert_eq!(MTLS_PRIVATE_KEY, "clickhouse.client.mtls_private_key");
        for key in [TLS_ROOT_CERTS, MTLS_CERT_CHAIN, MTLS_PRIVATE_KEY] {
            assert!(is_tls_option(key));
            assert!(key.starts_with("clickhouse.client."));
        }
        assert!(!is_tls_option("clickhouse.client.product_info"));
    }

    #[test]
    fn refuses_malformed_pem() {
        let mut opts = TlsOptions::default();
        let err = opts
            .try_set(TLS_ROOT_CERTS, "not a certificate".into())
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        let err = opts
            .try_set(
                MTLS_CERT_CHAIN,
                "-----BEGIN CERTIFICATE-----\n!!!!!!!!\n-----END CERTIFICATE-----".into(),
            )
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        let err = opts
            .try_set(MTLS_PRIVATE_KEY, "not a key".into())
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }

    #[test]
    fn accepts_valid_pem() {
        let mut opts = TlsOptions::default();
        opts.try_set(TLS_ROOT_CERTS, CA_PEM.into()).unwrap();
        opts.try_set(MTLS_CERT_CHAIN, CLIENT_CERT_PEM.into())
            .unwrap();
        opts.try_set(MTLS_PRIVATE_KEY, CLIENT_KEY_PEM.into())
            .unwrap();
        assert!(!opts.is_empty());
        opts.check_identity().unwrap();
    }

    #[test]
    fn refuses_half_identity() {
        let mut opts = TlsOptions::default();
        opts.try_set(MTLS_CERT_CHAIN, CLIENT_CERT_PEM.into())
            .unwrap();
        // Certificate chain without a private key is refused.
        let err = opts.check_identity().unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        let mut opts = TlsOptions::default();
        opts.try_set(MTLS_PRIVATE_KEY, CLIENT_KEY_PEM.into())
            .unwrap();
        // Private key without a certificate chain is refused.
        let err = opts.check_identity().unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }

    #[test]
    fn builds_https_connector() {
        let mut opts = TlsOptions::default();
        opts.try_set(TLS_ROOT_CERTS, CA_PEM.into()).unwrap();
        opts.try_set(MTLS_CERT_CHAIN, CLIENT_CERT_PEM.into())
            .unwrap();
        opts.try_set(MTLS_PRIVATE_KEY, CLIENT_KEY_PEM.into())
            .unwrap();
        opts.build_https_connector().unwrap();
    }
}
