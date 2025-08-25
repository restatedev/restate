// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper::Uri;
use restate_types::config::ProxyUri;
use rustls::pki_types::IpAddr;
use std::{
    collections::HashSet,
    task::{Context, Poll},
};
use tower_service::Service;

#[derive(Clone, Debug)]
pub struct ProxyConnector<C> {
    proxy: Option<ProxyUri>,
    no_proxy_ips: HashSet<IpAddr>,
    no_proxy_domains: Vec<String>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy: Option<ProxyUri>, no_proxy: Vec<http::uri::Authority>, connector: C) -> Self {
        let mut no_proxy_ips = HashSet::new();
        let mut no_proxy_domains = Vec::new();

        for no_proxy_authority in no_proxy {
            match IpAddr::try_from(no_proxy_authority.as_str()) {
                Ok(ip) => {
                    no_proxy_ips.insert(ip);
                }
                Err(_) => no_proxy_domains.push(no_proxy_authority.host().to_owned()),
            }
        }

        Self {
            proxy,
            no_proxy_ips,
            no_proxy_domains,
            connector,
        }
    }

    fn no_proxy(&self, host: &str) -> bool {
        // According to RFC3986, raw IPv6 hosts will be wrapped in []. So we need to strip those off
        // the end in order to parse correctly
        let authority = if host.starts_with('[') && host.ends_with(']') {
            &host[1..host.len() - 1]
        } else {
            host
        };
        match IpAddr::try_from(authority) {
            // If we can parse an IP addr, then use it, otherwise, assume it is a domain
            Ok(ip) => self.no_proxy_ip(ip),
            Err(_) => self.no_proxy_domain(authority),
        }
    }

    fn no_proxy_ip(&self, ip: IpAddr) -> bool {
        self.no_proxy_ips.contains(&ip)
    }

    // Copied from reqwest: https://github.com/seanmonstar/reqwest/blob/master/src/proxy.rs <dual-licensed Apache and MIT>
    // The following links may be useful to understand the origin of these rules:
    // * https://curl.se/libcurl/c/CURLOPT_NOPROXY.html
    // * https://github.com/curl/curl/issues/1208
    fn no_proxy_domain(&self, domain: &str) -> bool {
        let domain_len = domain.len();
        for d in &self.no_proxy_domains {
            let d = d.as_str();
            if d == domain || d.strip_prefix('.') == Some(domain) {
                return true;
            } else if domain.ends_with(d) {
                if d.starts_with('.') {
                    // If the first character of d is a dot, that means the first character of domain
                    // must also be a dot, so we are looking at a subdomain of d and that matches
                    return true;
                } else if domain.as_bytes().get(domain_len - d.len() - 1) == Some(&b'.') {
                    // Given that d is a prefix of domain, if the prior character in domain is a dot
                    // then that means we must be matching a subdomain of d, and that matches
                    return true;
                }
            } else if d == "*" {
                return true;
            }
        }
        false
    }
}

impl<C> Service<Uri> for ProxyConnector<C>
where
    C: Service<Uri>,
{
    type Response = C::Response;
    type Error = C::Error;
    type Future = C::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connector.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        if let Some(host) = uri.host()
            && self.no_proxy(host)
        {
            return self.connector.call(uri);
        }
        self.connector.call(match &self.proxy {
            Some(proxy) => proxy.dst(uri),
            None => uri,
        })
    }
}
