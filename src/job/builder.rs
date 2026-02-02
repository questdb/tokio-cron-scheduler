use crate::job::cron_job::CronJob;
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data;
#[cfg(not(feature = "has_bytes"))]
pub use crate::job::job_data::{JobStoredData, JobType, Uuid};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost;
#[cfg(feature = "has_bytes")]
pub use crate::job::job_data_prost::{JobStoredData, JobType, Uuid};
use crate::job::{JobLocked, nop, nop_async};
use crate::{JobSchedulerError, JobToRun, JobToRunAsync, SecondsMode};
use chrono::{Offset, TimeZone, Utc};
use core::time::Duration;
use croner::Cron;
use croner::parser::CronParser;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use uuid::Uuid as UuidUuid;

pub struct JobBuilder<T> {
    pub job_id: Option<Uuid>,
    pub timezone: Option<T>,
    pub job_type: Option<JobType>,
    pub schedule: Option<Cron>,
    pub run: Option<Box<JobToRun>>,
    pub run_async: Option<Box<JobToRunAsync>>,
    pub duration: Option<Duration>,
    pub repeating: Option<bool>,
    pub instant: Option<Instant>,
    pub seconds_mode: SecondsMode,
}

impl JobBuilder<Utc> {
    pub fn new() -> Self {
        Self {
            job_id: None,
            timezone: None,
            job_type: None,
            schedule: None,
            run: None,
            run_async: None,
            duration: None,
            repeating: None,
            instant: None,
            seconds_mode: SecondsMode::Required,
        }
    }
}

impl<T: TimeZone> JobBuilder<T> {
    pub fn with_timezone<U: TimeZone>(self, timezone: U) -> JobBuilder<U> {
        JobBuilder {
            timezone: Some(timezone),
            job_id: self.job_id,
            job_type: self.job_type,
            schedule: self.schedule,
            run: self.run,
            run_async: self.run_async,
            duration: self.duration,
            repeating: self.repeating,
            instant: self.instant,
            seconds_mode: self.seconds_mode,
        }
    }

    pub fn with_job_id(self, job_id: Uuid) -> Self {
        Self {
            job_id: Some(job_id),
            ..self
        }
    }

    /// Configure how the seconds field is handled in cron expressions.
    ///
    /// - `SecondsMode::Required` (default): 6-field expressions required (e.g., "0 */5 * * * *")
    /// - `SecondsMode::Optional`: 5 or 6 field expressions accepted (e.g., "*/5 * * * *" or "0 */5 * * * *")
    /// - `SecondsMode::Disallowed`: Only 5-field expressions accepted (e.g., "*/5 * * * *")
    pub fn with_seconds_mode(self, seconds_mode: SecondsMode) -> Self {
        Self {
            seconds_mode,
            ..self
        }
    }

    pub fn with_job_type(self, job_type: JobType) -> Self {
        Self {
            job_type: Some(job_type),
            ..self
        }
    }

    pub fn with_cron_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::Cron),
            ..self
        }
    }

    pub fn with_repeated_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::Repeated),
            ..self
        }
    }

    pub fn with_one_shot_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::OneShot),
            ..self
        }
    }

    pub fn with_schedule<TS>(self, schedule: TS) -> Result<Self, JobSchedulerError>
    where
        TS: ToString,
    {
        let seconds: croner::parser::Seconds = self.seconds_mode.into();
        let schedule = JobLocked::schedule_to_cron_with_seconds_mode(schedule, seconds)?;
        let schedule = CronParser::builder()
            .seconds(seconds)
            .build()
            .parse(&schedule)
            .map_err(|_| JobSchedulerError::ParseSchedule)?;

        Ok(Self {
            schedule: Some(schedule),
            ..self
        })
    }

    pub fn with_run_sync(self, job: Box<JobToRun>) -> Self {
        Self {
            run: Some(Box::new(job)),
            ..self
        }
    }

    pub fn with_run_async(self, job: Box<JobToRunAsync>) -> Self {
        Self {
            run_async: Some(Box::new(job)),
            ..self
        }
    }

    pub fn every_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(true),
            ..self
        }
    }

    pub fn after_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(false),
            ..self
        }
    }

    pub fn at_instant(self, instant: Instant) -> Self {
        Self {
            instant: Some(instant),
            ..self
        }
    }

    pub fn build(self) -> Result<JobLocked, JobSchedulerError> {
        if self.job_type.is_none() {
            return Err(JobSchedulerError::JobTypeNotSet);
        }
        let job_type = self.job_type.unwrap();
        let (run, run_async) = (self.run, self.run_async);
        if run.is_none() && run_async.is_none() {
            return Err(JobSchedulerError::RunOrRunAsyncNotSet);
        }
        let async_job = run_async.is_some();

        match job_type {
            JobType::Cron => {
                if self.schedule.is_none() {
                    return Err(JobSchedulerError::ScheduleNotSet);
                }
                let schedule = self.schedule.unwrap();

                let time_offset_seconds = if let Some(tz) = self.timezone.as_ref() {
                    tz.offset_from_utc_datetime(&Utc::now().naive_local())
                        .fix()
                        .local_minus_utc()
                } else {
                    0
                };

                Ok(JobLocked(Arc::new(RwLock::new(Box::new(CronJob {
                    data: JobStoredData {
                        id: self.job_id.or(Some(UuidUuid::new_v4().into())),
                        last_updated: None,
                        last_tick: None,
                        next_tick: match &self.timezone {
                            Some(timezone) => schedule
                                .find_next_occurrence(&Utc::now().with_timezone(timezone), false)
                                .map(|tz_time| tz_time.timestamp() as u64)
                                .unwrap_or(0),
                            None => schedule
                                .find_next_occurrence(&Utc::now(), false)
                                .map(|t| t.timestamp() as u64)
                                .unwrap_or(0),
                        },
                        job_type: JobType::Cron.into(),
                        count: 0,
                        extra: vec![],
                        ran: false,
                        stopped: false,
                        #[cfg(feature = "has_bytes")]
                        job: Some(job_data_prost::job_stored_data::Job::CronJob(
                            job_data_prost::CronJob {
                                schedule: schedule.pattern.to_string(),
                            },
                        )),
                        #[cfg(not(feature = "has_bytes"))]
                        job: Some(job_data::job_stored_data::Job::CronJob(job_data::CronJob {
                            schedule: schedule.pattern.to_string(),
                        })),
                        time_offset_seconds,
                    },
                    run: run.unwrap_or(Box::new(nop)),
                    run_async: run_async.unwrap_or(Box::new(nop_async)),
                    async_job,
                })))))
            }
            JobType::Repeated => Err(JobSchedulerError::NoNextTick),
            JobType::OneShot => Err(JobSchedulerError::NoNextTick),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{JobScheduler, SecondsMode};
    use chrono::Timelike;

    #[tokio::test]
    async fn test_timezone_job_builder() {
        let mut scheduler = JobScheduler::new().await.unwrap();

        let job_id = scheduler
            .add(
                JobBuilder::new()
                    .with_timezone(chrono_tz::Europe::Paris)
                    .with_cron_job_type()
                    .with_schedule("0 30 9 * * *")
                    .unwrap()
                    .with_run_async(Box::new(|_uuid, _lock| Box::pin(async move {})))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let next_tick = scheduler
            .next_tick_for_job(job_id)
            .await
            .unwrap()
            .expect("Should have next_tick");

        let paris_time = next_tick.with_timezone(&chrono_tz::Europe::Paris);
        assert_eq!(paris_time.hour(), 9);
        assert_eq!(paris_time.minute(), 30);
    }

    #[tokio::test]
    async fn test_five_field_cron_with_optional_seconds() {
        // 5-field expression should work with SecondsMode::Optional
        let mut scheduler = JobScheduler::new().await.unwrap();

        let job_id = scheduler
            .add(
                JobBuilder::new()
                    .with_cron_job_type()
                    .with_seconds_mode(SecondsMode::Optional)
                    .with_schedule("30 9 * * *") // 5-field: minute hour dom month dow
                    .unwrap()
                    .with_run_async(Box::new(|_uuid, _lock| Box::pin(async move {})))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let next_tick = scheduler
            .next_tick_for_job(job_id)
            .await
            .unwrap()
            .expect("Should have next_tick");

        // Should run at minute 30, hour 9
        assert_eq!(next_tick.minute(), 30);
        assert_eq!(next_tick.hour(), 9);
    }

    #[tokio::test]
    async fn test_five_field_cron_fails_with_required_seconds() {
        // 5-field expression should fail with default SecondsMode::Required
        let result = JobBuilder::new()
            .with_cron_job_type()
            .with_schedule("30 9 * * *"); // 5-field without setting Optional

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_six_field_cron_works_with_optional_seconds() {
        // 6-field expression should still work with SecondsMode::Optional
        let mut scheduler = JobScheduler::new().await.unwrap();

        let job_id = scheduler
            .add(
                JobBuilder::new()
                    .with_cron_job_type()
                    .with_seconds_mode(SecondsMode::Optional)
                    .with_schedule("0 30 9 * * *") // 6-field with seconds
                    .unwrap()
                    .with_run_async(Box::new(|_uuid, _lock| Box::pin(async move {})))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let next_tick = scheduler
            .next_tick_for_job(job_id)
            .await
            .unwrap()
            .expect("Should have next_tick");

        assert_eq!(next_tick.second(), 0);
        assert_eq!(next_tick.minute(), 30);
        assert_eq!(next_tick.hour(), 9);
    }

    #[tokio::test]
    async fn test_five_field_cron_works_with_disallowed_seconds() {
        // 5-field expression should work with SecondsMode::Disallowed
        let mut scheduler = JobScheduler::new().await.unwrap();

        let job_id = scheduler
            .add(
                JobBuilder::new()
                    .with_cron_job_type()
                    .with_seconds_mode(SecondsMode::Disallowed)
                    .with_schedule("30 9 * * *") // 5-field: minute hour dom month dow
                    .unwrap()
                    .with_run_async(Box::new(|_uuid, _lock| Box::pin(async move {})))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let next_tick = scheduler
            .next_tick_for_job(job_id)
            .await
            .unwrap()
            .expect("Should have next_tick");

        // Should run at minute 30, hour 9
        assert_eq!(next_tick.minute(), 30);
        assert_eq!(next_tick.hour(), 9);
    }

    #[tokio::test]
    async fn test_six_field_cron_fails_with_disallowed_seconds() {
        // 6-field expression should fail with SecondsMode::Disallowed
        let result = JobBuilder::new()
            .with_cron_job_type()
            .with_seconds_mode(SecondsMode::Disallowed)
            .with_schedule("0 30 9 * * *"); // 6-field should be rejected

        assert!(result.is_err());
    }
}
