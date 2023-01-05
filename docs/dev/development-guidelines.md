# Development guidelines

This document describes the general guidelines we are following when developing it.
This document should be updated whenever there are good new guidelines.

## When to open pull requests

For all non-trivial changes (e.g. correcting of typos) we recommend opening a PR and request a review before merging it.
This procedure has the advantage that bugs are less likely, and it helps to spread knowledge.

## Testing your changes

We recommend writing tests for new changes.
This has the advantage that bugs are less likely, and it guards against future changes that might break contracts. 

## Merging of changes

When merging changes, we do rebase onto the latest master and then push the changes.
This procedure ensures a clean branch history.
Consequently, merge commits are discouraged.

## Commit messages

Commit messages help to give context to a change.
Especially, when trying to understand why something was done the way it is done, this information can be really helpful.
Therefore, we encourage everyone to write proper and meaningful commit messages.
See this discussion for [how to write helpful commit messages](https://who-t.blogspot.com/2009/12/on-commit-messages.html).

## Linking pull requests to issues

Whenever a pull request is created to fix an issue, we recommend linking to it.
See [how to link pull requests to issues on Github](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue).

