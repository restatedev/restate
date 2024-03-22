use crate::cli_env::CliEnv;
use crate::clients::{MetaClientInterface, MetasClient};
use crate::ui::console::StyledTable;
use anyhow::{anyhow, bail, Context};
use arrow::array::{BinaryArray, StringArray};
use base64::alphabet::URL_SAFE;
use base64::engine::{Engine, GeneralPurpose, GeneralPurposeConfig};
use bytes::Bytes;
use comfy_table::{Cell, Table};
use itertools::Itertools;
use restate_meta_rest_model::components::{ComponentType, ModifyComponentStateRequest};
use restate_types::state_mut::StateMutationVersion;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub(crate) async fn get_current_state(
    env: &CliEnv,
    service: &str,
    key: &str,
) -> anyhow::Result<HashMap<String, Bytes>> {
    //
    // 0. require that this is a keyed service
    //
    let client = MetasClient::new(env)?;
    let service_meta = client.get_component(service).await?.into_body().await?;
    if service_meta.ty != ComponentType::VirtualObject {
        bail!("Only virtual objects support state");
    }
    //
    // 1. get the key-value pairs
    //
    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    let sql = format!(
        "select key, value from state where component = '{}' and component_key = '{}' ;",
        service, key
    );
    let res = sql_client.run_query(sql).await?;
    //
    // 2. convert the state to a map from str keys -> byte values.
    //
    let mut user_state = HashMap::new();
    for batch in res.batches {
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        user_state.reserve(n);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("bug: unexpected column type");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("bug: unexpected column type");
        for i in 0..n {
            let key = keys.value(i).to_string();
            let val = Bytes::copy_from_slice(vals.value(i));
            user_state.insert(key, val);
        }
    }
    Ok(user_state)
}

pub(crate) async fn update_state(
    env: &CliEnv,
    expected_version: Option<String>,
    service: &str,
    service_key: &str,
    new_state: HashMap<String, Bytes>,
) -> anyhow::Result<()> {
    let req = ModifyComponentStateRequest {
        version: expected_version,
        new_state,
        object_key: service_key.to_string(),
    };

    let client = MetasClient::new(env)?;
    client.patch_state(service, req).await.unwrap();

    Ok(())
}

pub(crate) fn compute_version(user_state: &HashMap<String, Bytes>) -> String {
    let kvs: Vec<(Bytes, Bytes)> = user_state
        .iter()
        .map(|(k, v)| (Bytes::from(k.clone()), v.clone()))
        .collect();
    StateMutationVersion::from_user_state(&kvs).into_inner()
}

pub(crate) fn as_json(state: HashMap<String, Bytes>, binary_values: bool) -> anyhow::Result<Value> {
    let current_state_json: HashMap<String, Value> = state
        .into_iter()
        .map(|(k, v)| bytes_as_json(v, binary_values).map(|v| (k, v)))
        .try_collect()?;

    serde_json::to_value(current_state_json).context("unable to create a JSON object.")
}

pub(crate) fn from_json(json: Value, binary_value: bool) -> anyhow::Result<HashMap<String, Bytes>> {
    let modified_state: HashMap<String, Bytes> = json
        .as_object()
        .expect("cli bug this must be an object")
        .into_iter()
        .map(|(k, v)| {
            let binary = json_value_as_bytes(v, binary_value);

            binary.map(|v| (k.clone(), v))
        })
        .try_collect()?;

    Ok(modified_state)
}

fn bytes_as_json(value: Bytes, binary_values: bool) -> anyhow::Result<Value> {
    let json: Value = if binary_values {
        let b64 = GeneralPurpose::new(&URL_SAFE, GeneralPurposeConfig::default()).encode(value);
        serde_json::to_value(b64).context("unable to convert bytes to string")?
    } else {
        serde_json::from_slice(&value).context("unable to convert a value to json")?
    };

    Ok(json)
}

/// convert a JSON value to bytes. If the original value was base64 encoded (binary_value = true)
/// then, the value will be a json string of the form " ... base64 encoded ... ", and it would be converted
/// to bytes by decoding the string.
/// if binary_value = false, we use serde json to decode this value.
fn json_value_as_bytes(value: &Value, binary_value: bool) -> anyhow::Result<Bytes> {
    let raw = if binary_value {
        base64_json_value_str_as_bytes(value)?
    } else {
        serde_json::to_vec(&value).context("unable to convert a JSON value back to bytes")?
    };

    Ok(Bytes::from(raw))
}

/// convert a JSON string value i.e. "abcde121==" that represents a base64 string
/// into a raw bytes (base64 decoded)
fn base64_json_value_str_as_bytes(value: &Value) -> anyhow::Result<Vec<u8>> {
    let str = value
        .as_str()
        .ok_or_else(|| anyhow!("unexpected non string value with binary mode"))?;

    GeneralPurpose::new(&URL_SAFE, GeneralPurposeConfig::default())
        .decode(str)
        .context("unable to decode a base64 value")
}

pub(crate) fn write_json_file(path: &Path, json: Value) -> anyhow::Result<()> {
    let current_json =
        serde_json::to_string_pretty(&json).context("Failed to serialize to JSON")?;

    let mut file = File::create(path).context("Failed to create a temp file")?;
    file.write_all(current_json.as_bytes())
        .context("Failed to write to file")?;
    file.sync_all()
        .context("unable to flush the file to disk")?;

    Ok(())
}

pub(crate) fn read_json_file(path: &Path) -> anyhow::Result<Value> {
    let mut file = File::open(path).context("Unable to open the file for reading")?;
    let mut json_str = String::new();
    file.read_to_string(&mut json_str)
        .context("Unable to read back the content of the file")?;

    let value: Value = serde_json::from_str(&json_str).context("Failed parsing JSON")?;

    if value.is_object() {
        Ok(value)
    } else {
        Err(anyhow!("expected to read back a JSON object"))
    }
}

pub(crate) fn pretty_print_json(env: &CliEnv, value: &Value) -> anyhow::Result<Table> {
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["KEY", "VALUE"]);

    let object = pretty_print_json_object(value)?;

    for (k, v) in object {
        table.add_row(vec![Cell::new(k), Cell::new(v)]);
    }

    Ok(table)
}

pub(crate) fn pretty_print_json_object(value: &Value) -> anyhow::Result<HashMap<String, String>> {
    assert!(value.is_object());

    let value = value
        .as_object()
        .expect("cli bug, this needs to be an object");

    value
        .into_iter()
        .map(|(k, v)| {
            let pretty_val =
                serde_json::to_string_pretty(v).context("unable convert a value to JSON")?;
            Ok((k.clone(), pretty_val))
        })
        .try_collect()
}
