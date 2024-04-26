use crate::{c_success, cli_env::CliEnv, ui::watcher::Watch};
use anyhow::Result;
use base64::Engine;
use cling::prelude::*;
use serde::{Deserialize, Serialize};
use toml_edit::{table, value, DocumentMut};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
pub struct Login {
    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_login(State(env): State<CliEnv>, opts: &Login) -> Result<()> {
    opts.watch.run(|| login(&env)).await
}

async fn login(env: &CliEnv) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data.parse::<DocumentMut>()?;

    let access_token = auth_flow(env)?;

    write_access_token(&mut doc, &access_token)?;

    // write out config
    std::fs::write(env.config_file.as_path(), doc.to_string())?;
    c_success!(
        "Updated {} with Restate Cloud credentials",
        env.config_file.display()
    );

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    exp: i64,
}

fn auth_flow(_env: &CliEnv) -> Result<String> {
    Ok(format!(
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{}.ScqUsLzCQpB4meQpW3HaHoKQmIxO3jLndblXL9VXckc",
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(
            serde_json::to_string(&TokenClaims {
                exp: i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        + 60
                )
                .unwrap()
            })
            .unwrap()
        )
    ))
}

fn write_access_token(doc: &mut DocumentMut, access_token: &str) -> Result<()> {
    let claims = match access_token
        .split('.')
        .nth(1)
        .map(|claims: &str| -> Result<TokenClaims> {
            Ok(serde_json::from_slice(
                &base64::prelude::BASE64_URL_SAFE_NO_PAD.decode(claims)?,
            )?)
        }) {
        Some(Ok(claims)) => claims,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid access token; could not parse expiry"
            ))
        }
    };

    let cloud = doc["global"].or_insert(table())["cloud"].or_insert(table());
    cloud["access_token"] = value(access_token);
    cloud["access_token_expiry"] = value(claims.exp);

    Ok(())
}
