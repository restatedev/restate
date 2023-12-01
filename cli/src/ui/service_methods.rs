use super::console::{Icon, StyledTable};
use crate::app::UiConfig;

use restate_meta_rest_model::services::{InstanceType, MethodMetadata};

use comfy_table::{Cell, Color, Table};

pub fn create_service_methods_table(
    ui_config: &UiConfig,
    service_type: InstanceType,
    methods: &[MethodMetadata],
) -> Table {
    let mut table = Table::new_styled(ui_config);

    let mut headers = vec!["NAME", "INPUT TYPE", "OUTPUT TYPE"];
    if service_type != InstanceType::Unkeyed {
        headers.push("KEY FIELD INDEX");
    }
    table.set_styled_header(headers);

    for method in methods {
        let mut row = vec![
            Cell::new(&method.name),
            Cell::new(&method.input_type),
            Cell::new(&method.output_type),
        ];
        if service_type != InstanceType::Unkeyed {
            row.push(Cell::new(
                &method
                    .key_field_number
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            ));
        }
        table.add_row(row);
    }
    table
}

pub fn create_service_methods_table_diff(
    ui_config: &UiConfig,
    service_type: InstanceType,
    old_svc_methods: &[MethodMetadata],
    new_svc_methods: &[MethodMetadata],
) -> Table {
    let mut old_svc_methods = old_svc_methods
        .iter()
        .map(|m| (m.name.clone(), m))
        .collect::<std::collections::HashMap<_, _>>();

    let mut table = Table::new_styled(ui_config);
    let mut headers = vec!["", "NAME", "INPUT TYPE", "OUTPUT TYPE"];

    if service_type != InstanceType::Unkeyed {
        headers.push("KEY FIELD INDEX");
    }
    table.set_styled_header(headers);

    // Additions and updates
    for method in new_svc_methods {
        let mut row = vec![];
        if old_svc_methods.remove(&method.name).is_some() {
            // possibly updated.
            row.push(Cell::new(""));
            row.push(Cell::new(&method.name));
        } else {
            // new method
            row.push(Cell::new("++").fg(Color::Green));
            row.push(Cell::new(&method.name).fg(Color::Green));
        }
        row.extend_from_slice(&[
            Cell::new(&method.input_type),
            Cell::new(&method.output_type),
        ]);
        if service_type != InstanceType::Unkeyed {
            row.push(Cell::new(
                &method
                    .key_field_number
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            ));
        }
        table.add_row(row);
    }

    // Removals
    for method in old_svc_methods.values() {
        let mut row = vec![
            Cell::new("--").fg(Color::Red),
            Cell::new(&method.name).fg(Color::Red),
            Cell::new(&method.input_type),
            Cell::new(&method.output_type),
        ];

        if service_type != InstanceType::Unkeyed {
            row.push(Cell::new(
                &method
                    .key_field_number
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            ));
        }
        table.add_row(row);
    }
    table
}

pub fn icon_for_service_flavor(svc_type: &InstanceType) -> Icon {
    match svc_type {
        InstanceType::Unkeyed => Icon("", ""),
        InstanceType::Keyed => Icon("⬅️ 🚶🚶🚶", "keyed"),
        InstanceType::Singleton => Icon("👑", "singleton"),
    }
}

pub fn icon_for_is_public(public: bool) -> Icon<'static, 'static> {
    if public {
        Icon("🌎", "[public]")
    } else {
        Icon("🔒", "[private]")
    }
}
