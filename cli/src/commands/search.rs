// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use anyhow::Result;
use bm25::{Language, SearchEngine, SearchEngineBuilder, Tokenizer};
use clap::CommandFactory;
use cling::prelude::*;

use crate::app::CliApp;
use crate::commands::sql::SQL_TABLES;
use crate::ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter};

/// Maximum number of results shown for a query.
const LIMIT: usize = 10;
/// Minimum Jaro-Winkler similarity for a typo-tolerant token match.
const FUZZY_THRESHOLD: f64 = 0.9;
/// Boosts of [`Entry::fields`].
const BOOSTS: [f32; 4] = [8.0, 5.0, 2.0, 0.5];

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_search")]
pub struct Search {
    /// Words to look for, or a description of what you want to do. Typos are tolerated.
    #[arg(required = true)]
    query: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Command,
    SqlTable,
}

/// A searchable command or SQL table.
struct Entry {
    kind: Kind,
    /// What to run: `restate invocations cancel`, `restate sql describe <table>`.
    command: String,
    /// One-line description.
    description: String,
    /// The searched fields: names, summary, description and context (flags, columns).
    fields: [String; 4],
}

fn run_search(opts: &Search) -> Result<()> {
    let index = build_index();
    let query = opts.query.join(" ");
    let results = Searcher::new(&index).search(&query);

    let rows: Vec<[Field; 2]> = results
        .iter()
        .map(|e| {
            [
                Field::new(e.command.as_str()),
                Field::new(e.description.as_str()),
            ]
        })
        .collect();
    let mut f = Formatter::new();
    f.table(
        "results",
        &["command", "description"],
        &rows,
        IfEmpty::Say(&format!("No commands or SQL tables match '{query}'.")),
    );
    match results.first() {
        None => {
            f.next_step("restate --help", "list the commands", IncludeFormatting::No);
        }
        Some(top) if top.kind == Kind::SqlTable => {
            f.next_step(&top.command, "see its columns", IncludeFormatting::Yes)
        }
        Some(top) => f.next_step(
            &format!("{} --help", top.command),
            "see its usage",
            IncludeFormatting::No,
        ),
    }
    f.finish()
}

/// Every non-hidden command of the live CLI, then the SQL tables. A command's arguments
/// and flags are searchable as part of the command itself.
fn build_index() -> Vec<Entry> {
    let mut entries = Vec::new();
    index_command(&CliApp::command(), "restate".to_owned(), &mut entries);
    for table in SQL_TABLES {
        let columns: Vec<&str> = table.columns.iter().map(|c| c.name).collect();
        entries.push(Entry {
            kind: Kind::SqlTable,
            command: format!("restate sql describe {}", table.name),
            description: if table.description.is_empty() {
                format!("SQL table with {} columns", table.columns.len())
            } else {
                first_line(table.description)
            },
            fields: searchable([table.name, table.description, "", &columns.join(" ")]),
        });
    }
    entries
}

fn index_command(cmd: &clap::Command, path: String, entries: &mut Vec<Entry>) {
    let mut names = path.trim_start_matches("restate").to_owned();
    for alias in cmd.get_all_aliases() {
        names.push(' ');
        names.push_str(alias);
    }
    let mut context = String::new();
    for arg in cmd.get_arguments().filter(|a| !a.is_hide_set()) {
        let values = arg.get_possible_values();
        let words = std::iter::once(arg.get_id().as_str())
            .chain(arg.get_all_aliases().into_iter().flatten())
            .chain(
                values
                    .iter()
                    .filter(|v| !v.is_hide_set())
                    .map(|v| v.get_name()),
            );
        for word in words {
            context.push_str(word);
            context.push(' ');
        }
        if let Some(help) = arg.get_help() {
            context.push_str(&help.to_string());
            context.push(' ');
        }
    }
    let about = cmd.get_about().map(ToString::to_string).unwrap_or_default();
    let long_about = cmd
        .get_long_about()
        .map(ToString::to_string)
        .unwrap_or_default();
    entries.push(Entry {
        kind: Kind::Command,
        command: path.clone(),
        description: first_line(&about),
        fields: searchable([&names, &about, &long_about, &context]),
    });
    for sub in cmd.get_subcommands() {
        if !sub.is_hide_set() && sub.get_name() != "help" {
            index_command(sub, format!("{path} {}", sub.get_name()), entries);
        }
    }
}

/// `fields` with their compound words split, see [`tokens`].
fn searchable(fields: [&str; 4]) -> [String; 4] {
    fields.map(|f| tokens(f).join(" "))
}

/// BM25 ranking of each of [`Entry::fields`], summed with [`BOOSTS`], using the English
/// stemmer and stop words. Prefixes and typos are tolerated: a query word that matches
/// nothing is replaced by the closest words of the index.
struct Searcher<'a> {
    index: &'a [Entry],
    engines: Vec<SearchEngine<usize>>,
    tokenizer: bm25::DefaultTokenizer,
    /// Every word of the index.
    words: BTreeSet<String>,
    /// Every word of the index, as the tokenizer indexed it.
    terms: BTreeSet<String>,
}

impl<'a> Searcher<'a> {
    fn new(index: &'a [Entry]) -> Self {
        let tokenizer = bm25::DefaultTokenizer::new(Language::English);
        let words: BTreeSet<String> = index
            .iter()
            .flat_map(|e| e.fields.iter().flat_map(|f| f.split_whitespace()))
            .map(str::to_owned)
            .collect();
        let terms = words.iter().flat_map(|w| tokenizer.tokenize(w)).collect();
        let engines = (0..BOOSTS.len())
            .map(|field| {
                SearchEngineBuilder::with_documents(
                    Language::English,
                    index
                        .iter()
                        .enumerate()
                        .map(|(id, e)| bm25::Document::new(id, e.fields[field].as_str())),
                )
                .build()
            })
            .collect();
        Self {
            index,
            engines,
            tokenizer,
            words,
            terms,
        }
    }

    /// The best entries for `query`, best first. Ties keep the index order.
    fn search(&self, query: &str) -> Vec<&'a Entry> {
        let mut query_words = Vec::new();
        for word in tokens(query) {
            let known = self
                .tokenizer
                .tokenize(&word)
                .iter()
                .all(|t| self.terms.contains(t));
            if known {
                query_words.push(word);
            } else {
                query_words.extend(self.closest(&word).map(str::to_owned));
            }
        }
        let query = query_words.join(" ");
        let mut scores = vec![0.0; self.index.len()];
        for (engine, boost) in self.engines.iter().zip(BOOSTS) {
            for r in engine.search(&query, None) {
                scores[r.document.id] += boost * r.score;
            }
        }
        let mut ranked: Vec<usize> = (0..scores.len()).filter(|&i| scores[i] > 0.0).collect();
        ranked.sort_by(|&a, &b| scores[b].total_cmp(&scores[a]));
        ranked
            .into_iter()
            .take(LIMIT)
            .map(|i| &self.index[i])
            .collect()
    }

    /// The index words most similar to `word`, if any is similar at all.
    fn closest(&self, word: &str) -> impl Iterator<Item = &str> {
        let best = self
            .words
            .iter()
            .map(|w| similarity(word, w))
            .fold(0.0, f64::max);
        self.words
            .iter()
            .filter(move |w| best > 0.0 && similarity(word, w) == best)
            .map(String::as_str)
    }
}

/// 1 for an exact match, less for a prefix or a typo, 0 for no match.
fn similarity(query: &str, token: &str) -> f64 {
    if query == token {
        1.0
    } else if query.len() >= 3 && token.starts_with(query) {
        0.9
    } else if query.len() >= 4
        && token.len() >= 4
        && strsim::jaro_winkler(query, token) >= FUZZY_THRESHOLD
    {
        0.8
    } else {
        0.0
    }
}

/// Lowercase words of `text`; compound words (`kafka-clusters`, `sys_invocation`) are
/// kept whole and also split into their parts.
fn tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for word in text.split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '_')) {
        let word = word.trim_matches(['-', '_']).to_lowercase();
        if word.contains(['-', '_']) {
            out.extend(
                word.split(['-', '_'])
                    .filter(|p| !p.is_empty())
                    .map(str::to_owned),
            );
        }
        if !word.is_empty() {
            out.push(word);
        }
    }
    out
}

/// The first sentence of `text`'s first paragraph, with wrapped lines joined and a
/// trailing lead-in to a list (`..., e.g.:`) dropped.
fn first_line(text: &str) -> String {
    let paragraph = text
        .lines()
        .map(str::trim)
        .skip_while(|l| l.is_empty())
        .take_while(|l| !l.is_empty() && !l.starts_with(['*', '-']))
        .collect::<Vec<_>>()
        .join(" ");
    let end = paragraph
        .match_indices(". ")
        .map(|(i, _)| i)
        .find(|&i| !paragraph[..i].ends_with("e.g") && !paragraph[..i].ends_with("i.e"))
        .unwrap_or(paragraph.len());
    let sentence = &paragraph[..end];
    sentence
        .split_once("e.g.:")
        .map_or(sentence, |(lead, _)| lead)
        .trim_end_matches([',', ' ', ':', '.'])
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranks_commands_and_tables() {
        let index = build_index();
        let searcher = Searcher::new(&index);
        let top = |query: &str| {
            let e = searcher.search(query)[0];
            (e.command.as_str(), e.kind)
        };
        assert_eq!(
            top("cancel invocations"),
            ("restate invocations cancel", Kind::Command)
        );
        assert_eq!(top("cancle"), ("restate invocations cancel", Kind::Command));
        assert_eq!(
            top("sys_invocation"),
            ("restate sql describe sys_invocation", Kind::SqlTable)
        );
        let commands: Vec<_> = searcher
            .search("cancel invocations")
            .iter()
            .map(|e| e.command.as_str())
            .collect();
        assert!(commands.contains(&"restate invocations kill"));
        assert_eq!(
            first_line("Either an id, or a prefix, e.g.:\n* `id`\n* `prefix`"),
            "Either an id, or a prefix"
        );
        assert!(searcher.search("xyzzyqux").is_empty());
        assert!(index.iter().all(|e| !e.command.contains(" help")));
    }
}
