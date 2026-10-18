# Avoid duplicate cancellation notifications

## Bug Fix

Repeated cancellation requests for an in-flight invocation using journal v2 no
longer append duplicate cancellation signals or notify the handler again. This
prevents a repeated request from interrupting cleanup started after the first
cancellation. If the invocation has since been paused, another cancellation request
still resumes it without adding another cancellation signal.
