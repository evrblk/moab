# Schedules

You can setup one or more schedules at which Moab will enqueue tasks into a queue for you. Schedules belong to a single
queue only. You can continue enqueuing tasks into a queue with a schedule normally.

Schedule is set in form of a `cron` expression with a required `timezone`. Each schedule under the hood will basically 
call `Enqueue` with parameters `payload`, `dedupe_key`, `expires_in_seconds`, `keepalive_timeout_in_seconds`, and 
`retry_strategy` set from that schedule. `payload` is always the same. `dedupe_key` can be used to avoid a situation 
when a previous task is still in the queue and has not been processed yet, but the next tick of the schedule has already 
arrived (see more in [Unique Tasks](/docs/unique-tasks.md)). `expires_in_seconds`, `keepalive_timeout_in_seconds`, and 
`retry_strategy` can be used to override the default values set on the queue itself.

Internally, the scheduler works periodically by looking a few minutes ahead of the schedule and enqueuing tasks with
specified `scheduled_at` timestamp. This way tasks appear in the queue exactly according to specified schedule, 
regardless of precision and frequency at which the scheduler runs.

## Cron Expressions

A complete cron expression consists of 7 segments: `<second> <minute> <hour> <day> <month> <weekday> <year>`. However, 
just 5 will do and this is most commonly used. 5 segments are interpreted as: `<minute> <hour> <day> <month> <weekday>`.
In this case a default value of 0 is prepended for `<second>` position. In a 6 segments expression, if 6th segment 
matches `<year>` (4 digits at least) it will be interpreted as: `<minute> <hour> <day> <month> <weekday> <year>` 
and a default value of 0 is prepended for `<second>` position.

* For each segment you can have multiple choices separated by comma: `0 0,30 * * * *` means either 0th or 30th minute.
* To specify range of values you can use dash: `0 10-15 * * * *` means 10th, 11th, 12th, 13th, 14th and 15th minute.
* To specify range of step you can combine a dash and slash: `0 10-15/2 * * * *` means every 2 minutes between 10 and 
  15, i.e 10th, 12th and 14th minute.
* For the `<day>` and `<weekday>` segment, there are additional modifiers (optional):
  * Day of Month (3rd of 5 segments or 4th of 6+ segments):
    * `L` stands for last day of month (`L` could mean 29th for February in leap year)
    * `W` stands for closest week day (`10W` is the closest week days (MON-FRI) to 10th date)
  * Day of Week (5th of 5 segments or  6th of 6+ segments):
    * `L` stands for last weekday of month (`2L` is last tuesday)
    * `#` stands for nth day of week in the month (`1#2` is second monday)
* You can mix the multiple choices, ranges and steps in a single expression: `0 5,12-20/4,55 * * * *` matches if any 
  one of 5 or 12-20/4 or 55 matches the minute.
