﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using EventStore.Client;

namespace CObs
{
    public interface IReadActionResult
    {
        bool                   Success            { get; }
        string                 Message            { get; }
        SourceValidationStatus Status             { get; }
        ulong                  ReadPosition       { get; }
        Uuid                   LastSeenCheckpoint { get; }
        DateTime               BuildFrom          { get; }
    }

    public interface IReadEventsAsync
    {
        Task<IReadActionResult> ReadDaysRawAsync(BaseDays pBaseDays);
    }

    public interface ICommitActionResult
    {
        bool   Success { get; }
        string Message { get; }
    }

    public interface ICommitEventsAsync
    {
        Task<bool>                IsBuildDequeued();
        Task<ICommitActionResult> RegisterBuild();
        Task<ICommitActionResult> CommitResultsAsync(
             List<BuildJob> pBuildQueue
            ,ulong          pReadPosition
            ,Uuid           pCheckpoint
            ,int            pMinSeriesIndex
            ,int            pBuildFromIndex
            ,int            pMaxSeriesIndex
        );
    }

    class ReadActionResult : IReadActionResult
    {
        public bool                   Success            { get; private set; }
        public string                 Message            { get; private set; }
        public SourceValidationStatus Status             { get; private set; }
        public ulong                  ReadPosition       { get; private set; }
        public Uuid                   LastSeenCheckpoint { get; private set; }
        public DateTime               BuildFrom          { get; private set; }

        public ReadActionResult(
             bool                   pSuccess
            ,string                 pMessage
            ,SourceValidationStatus pStatus
            ,ulong                  pReadPosition
            ,Uuid                   pLastSeenCheckpoint
            ,DateTime               pBuildFrom
        ) {
            Success            = pSuccess;
            Status             = pSuccess
                ? new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                : pStatus;
            Message            = pSuccess ? ""                  : pMessage;
            ReadPosition       = pSuccess ? pReadPosition       : 0;
            LastSeenCheckpoint = pSuccess ? pLastSeenCheckpoint : Uuid.Empty;
            BuildFrom          = pSuccess ? pBuildFrom          : DateTime.Today;
        }
    }

    class CommitActionResult : ICommitActionResult
    {
        public bool   Success { get; private set; }
        public string Message { get; private set; }

        public CommitActionResult(
             bool         pSuccess
            ,string       pMessage
        ) {
            Success = pSuccess;
            Message = pSuccess ? "" : pMessage;
        }
    }

    public class CheckpointClear
    {
        public string CheckpointID { get; private set; }

        public CheckpointClear(Uuid pCheckpointID) {
            CheckpointID = pCheckpointID.ToString();
        }
    }

    public class SourceFromFile : IReadEventsAsync
    {
        public async Task<IReadActionResult> ReadDaysRawAsync(BaseDays pBaseDays)
        {
            DateTime today      = DateTime.Today;
            bool     contiguous = true;
            int      index      = 0;

            try
            {
                /* read raw source data */
                using var sr = new StreamReader("SourceData.txt");
                
                string? line;

                while ((line = await sr.ReadLineAsync()) != null)
                {
                    List<string> row = line.Split(',').ToList().Select(
                        entry => entry.Trim()
                    ).ToList();

                    SourceRowValidationStatus status = pBaseDays.AddDay(row);

                    if (status != SourceRowValidationStatus.OK)
                    {
                        /* report validation error */
                        return new ReadActionResult(
                             false
                            ,"row validation error"
                            ,new SourceValidationStatus(
                                 false
                                ,(index + 1) /* line number hints start from 1 */
                                ,status
                            )
                            ,0
                            ,Uuid.Empty
                            ,today
                        );
                    }

                    index++;
                }

                contiguous = pBaseDays.ValidateTimelineContiguityAndSeedIndex();

                if (!contiguous)
                {
                    /* report contiguity error */
                    return new ReadActionResult(
                         false
                        ,"timeline not contiguous"
                        ,new SourceValidationStatus(
                             true
                            ,0
                            ,SourceRowValidationStatus.OK
                        )
                        ,0
                        ,Uuid.Empty
                        ,today
                    );
                }
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,0
                    ,Uuid.Empty
                    ,today
                );
            }

            /* return souce events */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,0
                ,Uuid.Empty
                ,today
            );
        }
    }

    public class ResultsToFile : ICommitEventsAsync
    {
        public Task<bool> IsBuildDequeued()
        {
            return Task.FromResult(false);
        }

        public Task<ICommitActionResult> RegisterBuild()
        {
            return Task.FromResult(
                new CommitActionResult(true, "") as ICommitActionResult
            );
        }

        private static async Task<ICommitActionResult> CommitResultsDaysAsync(
            List<ResultsDay> pResultsDays
        ) {
            try
            {
                using (var w = new StreamWriter("ResultsData.txt"))
                {
                    foreach (ResultsDay day in pResultsDays)
                    {
                        string line
                          = day.TimelineIndex
                          + " ,"
                          + day.Date.ToString("yyyy-MM-dd")
                          + " ,"
                          + day.Mortality
                          + " ,"
                          + day.Hospitalizations
                          + " ,"
                          + day.Tests
                          + " ,"
                          + day.Positivity
                          + " ,"
                          + day.Rolling5DayMortality
                          + " ,"
                          + day.Rolling5DayHospitalizations
                          + " ,"
                          + day.Rolling5DayTests
                          + " ,"
                          + day.Rolling5DayPositivity
                          + " ,"
                          + (int)day.LowerBoundSourcedOn
                          + " ,"
                          + (int)day.BaselineSourcedOn
                          + " ,"
                          + (int)day.UpperBoundSourcedOn
                          + " ,"
                          + day.AdmissionsWithChurnLowerBound
                          + " ,"
                          + day.AdmissionsWithChurnBaseline
                          + " ,"
                          + day.AdmissionsWithChurnUpperBound
                          + " ,"
                          + day.ActualDNCLowerBound
                          + " ,"
                          + day.ActualDNCBaseline
                          + " ,"
                          + day.ActualDNCUpperBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTLowerBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTBaseline
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTUpperBound
                          + " ,"
                          + day.GrowthRateLowerBound
                          + " ,"
                          + day.GrowthRateBaseline
                          + " ,"
                          + day.GrowthRateUpperBound
                          + " ,"
                          + day.REffLowerBound
                          + " ,"
                          + day.REffBaseline
                          + " ,"
                          + day.REffUpperBound
                          + " ,"
                          + day.DoublingTimeLowerBound
                          + " ,"
                          + day.DoublingTimeBaseline
                          + " ,"
                          + day.DoublingTimeUpperBound;

                        await w.WriteLineAsync(line);
                        await w.FlushAsync();
                    }

                    w.Close();
                }

                File.Copy("ResultsData.txt", "CObsResults/ResultsData.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing results to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        private static async Task<ICommitActionResult> CommitAggregatesAsync(
            Aggregates pAggregates
        ) {
            try
            {
                using (var w = new StreamWriter("Aggregates.txt"))
                {
                    string line
                      = pAggregates.CurrentREffLowerBound
                      + " ,"
                      + pAggregates.CurrentREffBaseline
                      + " ,"
                      + pAggregates.CurrentREffUpperBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeLowerBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeBaseline
                      + " ,"
                      + pAggregates.CurrentDoublingTimeUpperBound
                      + " ,"
                      + ((pAggregates.CurrentDoublingTimeUnstable) ? 1 : 0)
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevUpperBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityUpperBound;

                    await w.WriteLineAsync(line);
                    await w.FlushAsync();

                    w.Close();
                }

                File.Copy("Aggregates.txt", "CObsResults/Aggregates.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing aggregates to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        public async Task<ICommitActionResult> CommitResultsAsync(
             List<BuildJob> pBuildQueue
            ,ulong          pReadPosition
            ,Uuid           pCheckpoint
            ,int            pMinSeriesIndex
            ,int            pBuildFromIndex
            ,int            pMaxSeriesIndex
        ) {
            var result = await CommitResultsDaysAsync(pBuildQueue[0].ResultsDays);

            if (!result.Success) { return result; }

            return await CommitAggregatesAsync(pBuildQueue[0].Aggregates);
        }
    }

    public class SourceFromEvents : IReadEventsAsync
    {
        class SourceBatch
        {
            public int          Index      { get; private set; }
            public string       Checkpoint { get; private set; }
            public List<DayRaw> DaysRaw    { get; private set; }
            public bool         Handled    { get; set; }

            public SourceBatch(int pIndex, string pCheckpoint, List<DayRaw> pDaysRaw)
            {
                Index      = pIndex;
                Checkpoint = pCheckpoint;
                DaysRaw    = pDaysRaw;
                Handled    = false;
            }
        }

        public EventStoreClient Client     { get; private set; }
        public string           StreamName { get; private set; }

        public SourceFromEvents(
             EventStoreClient pClient
            ,string           pStreamName
        ) { Client = pClient; StreamName = pStreamName; }

        public static IReadActionResult ReportTypeException(
             int    pLineNumber
            ,string pMessage
        ) {
            return new ReadActionResult(
                 false
                ,pMessage
                ,new SourceValidationStatus(
                     false
                    ,pLineNumber
                    ,SourceRowValidationStatus.ExceptionReadingEvents
                )
                ,0
                ,Uuid.Empty
                ,DateTime.Today
            );
        }

        public async Task<IReadActionResult> ReadDaysRawAsync(BaseDays pBaseDays)
        {
            ulong                pos            = 0;
            DateTime             today          = DateTime.Today;
            var                  batches        = new List<SourceBatch>();
            var                  daysRaw        = new List<DayRaw>();
            var                  daysHandled    = new Dictionary<DateTime, DayRaw>();
            string               cleared        = "";
            Uuid                 lastCheckpoint = Uuid.Empty;
            int                  batchIndex     = 0;
            int                  index          = 0;
            bool                 contiguous;
            List<ResolvedEvent>? events;
            DateTime             buildFrom;

            try
            {
                var result = Client.ReadStreamAsync(
                     Direction.Forwards
                    ,StreamName + "-source"
                    ,StreamPosition.Start
                );

                if (await result.ReadState != ReadState.Ok)
                {
                    throw new Exception("source stream empty");
                }

                events = await result.ToListAsync();
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,0
                    ,Uuid.Empty
                    ,today
                );
            }

            if (events.Count == 0)
            {
                /* report type exception (no source) */
                return ReportTypeException(0, "no source data in stream");
            }

            foreach (var sourceEvent in events)
            {
                pos = sourceEvent.OriginalEventNumber.ToUInt64();

                if (sourceEvent.OriginalEvent.EventType == "checkpoint")
                {
                    lastCheckpoint = sourceEvent.OriginalEvent.EventId;

                    /* if no final checkpoint mark is present we ignore dirty reads */
                    batches.Add(new SourceBatch(
                         batchIndex
                        ,sourceEvent.OriginalEvent.EventId.ToString()
                        ,daysRaw
                    ));

                    daysRaw = new List<DayRaw>();

                    batchIndex++;
                }

                if (sourceEvent.OriginalEvent.EventType == "checkpoint-clear")
                {
                    try
                    {
                        CheckpointClearRoot clear = JsonSerializer.Deserialize<
                            CheckpointClearRoot
                        >(
                            Encoding.UTF8.GetString(sourceEvent.Event.Data.ToArray())!
                        )!;

                        cleared = clear.CheckpointID!;
                    }
                    catch (Exception e)
                    {
                        /* report type exception (bad checkpoint-clear) */
                        return ReportTypeException(0, e.Message);
                    }
                }

                if (sourceEvent.OriginalEvent.EventType != "source-day-received") { continue; }

                SourceDayRoot? day;

                try
                {
                    day = JsonSerializer.Deserialize<SourceDayRoot>(
                        Encoding.UTF8.GetString(sourceEvent.Event.Data.ToArray())!
                    )!;
                }
                catch (Exception e)
                {
                    /* report type exception (line number hints start from 1) */
                    return ReportTypeException(index + 1, e.Message);
                }

                SourceRowValidationStatus status = BaseDays.ValidateSourceEvent(day.sourceDay!);

                if (status != SourceRowValidationStatus.OK)
                {
                    /* report row validation error */
                    return new ReadActionResult(
                         false
                        ,"row validation error"
                        ,new SourceValidationStatus(
                             false
                            ,(index + 1) /* line number hints start from 1 */
                            ,status
                        )
                        ,0
                        ,Uuid.Empty
                        ,today
                    );
                }

                daysRaw.Add(new DayRaw(0, day.sourceDay!));

                index++;
            }

            foreach (var batch in batches) { pBaseDays.AddDaysByDate(batch.DaysRaw); }

            contiguous = pBaseDays.ValidateTimelineContiguityAndSeedIndex();

            if (!contiguous)
            {
                /* report contiguity error */
                return new ReadActionResult(
                     false
                    ,"timeline not contiguous"
                    ,new SourceValidationStatus(
                         true
                        ,0
                        ,SourceRowValidationStatus.OK
                    )
                    ,0
                    ,Uuid.Empty
                    ,today
                );
            }

            foreach (var batch in batches)
            {
                if (batch.Checkpoint == cleared)
                {
                    foreach (var handled in batches)
                    {
                        if (handled.Index <= batch.Index) { handled.Handled = true; }
                        else                              { break; }
                    }
                }
            }

            foreach (var batch in batches)
            {
                if (!batch.Handled) { break; }

                foreach (var day in batch.DaysRaw)
                {
                    if (daysHandled.ContainsKey(day.Date))
                    {
                        daysHandled[day.Date] = day;
                    }
                    else
                    {
                        daysHandled.Add(day.Date, day);
                    }
                }
            }

            buildFrom = pBaseDays.DaysRaw[0].Date;

            if (daysHandled.Count > 0)
            {
                buildFrom = (daysHandled.OrderBy(day => day.Key).Last().Key).AddDays(1);

                foreach (var batch in batches)
                {
                    if (batch.Handled) { continue; }

                    foreach (var day in batch.DaysRaw)
                    {
                        if (
                            (daysHandled.ContainsKey(day.Date))
                        &&  (!DayRaw.Compare(day, daysHandled[day.Date]))
                        &&  (day.Date < buildFrom)
                        ) { buildFrom = day.Date; }
                    }
                }
            }

            /* return souce events aggregates */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,pos
                ,lastCheckpoint
                ,buildFrom
            );
        }
    }

    public class ResultsToEvents : ICommitEventsAsync
    {
        class BuildEvent
        {
            public DateTime Timestamp { get; private set; }

            public BuildEvent() { Timestamp = DateTime.Now; }
        }

        class ResultsDayBySeries
        {
            #pragma warning disable IDE1006

            public string[] t { get; set; }

            #pragma warning restore IDE1006

            public ResultsDayBySeries(
                 Uuid       pBuildEventID
                ,BuildJob   pBuildJob
                ,ResultsDay pResultsDay
            ) {
                t = new string[] {
                     pBuildEventID.ToString()
                    ,pBuildJob.TimeSeriesIndex.ToString()
                    ,pBuildJob.TimeSeriesDay.ToString("yyyy-MM-dd")
                    ,pResultsDay.TimelineIndex.ToString()
                    ,pResultsDay.Date.ToString("yyyy-MM-dd")
                    ,pResultsDay.Mortality.ToString()
                    ,pResultsDay.Hospitalizations.ToString()
                    ,pResultsDay.Tests.ToString()
                    ,pResultsDay.Positivity.ToString()
                    ,pResultsDay.Rolling5DayMortality.ToString()
                    ,pResultsDay.Rolling5DayHospitalizations.ToString()
                    ,pResultsDay.Rolling5DayTests.ToString()
                    ,pResultsDay.Rolling5DayPositivity.ToString()
                    ,((int)pResultsDay.LowerBoundSourcedOn).ToString()
                    ,((int)pResultsDay.BaselineSourcedOn).ToString()
                    ,((int)pResultsDay.UpperBoundSourcedOn).ToString()
                    ,pResultsDay.AdmissionsWithChurnLowerBound.ToString()
                    ,pResultsDay.AdmissionsWithChurnBaseline.ToString()
                    ,pResultsDay.AdmissionsWithChurnUpperBound.ToString()
                    ,pResultsDay.ActualDNCLowerBound.ToString()
                    ,pResultsDay.ActualDNCBaseline.ToString()
                    ,pResultsDay.ActualDNCUpperBound.ToString()
                    ,pResultsDay.Rolling9DayDeltaCDeltaTLowerBound.ToString()
                    ,pResultsDay.Rolling9DayDeltaCDeltaTBaseline.ToString()
                    ,pResultsDay.Rolling9DayDeltaCDeltaTUpperBound.ToString()
                    ,pResultsDay.GrowthRateLowerBound.ToString()
                    ,pResultsDay.GrowthRateBaseline.ToString()
                    ,pResultsDay.GrowthRateUpperBound.ToString()
                    ,pResultsDay.REffLowerBound.ToString()
                    ,pResultsDay.REffBaseline.ToString()
                    ,pResultsDay.REffUpperBound.ToString()
                    ,pResultsDay.DoublingTimeLowerBound.ToString()
                    ,pResultsDay.DoublingTimeBaseline.ToString()
                    ,pResultsDay.DoublingTimeUpperBound.ToString()
                };
            }
        }

        class AggregatesBySeries
        {
            public string     BuildEventID    { get; private set; }
            public int        TimeSeriesIndex { get; private set; }
            public DateTime   TimeSeriesDay   { get; private set; }
            public Aggregates Aggregates      { get; private set; }

            public AggregatesBySeries(Uuid pBuildEventID, BuildJob pBuildJob)
            {
                BuildEventID    = pBuildEventID.ToString();
                TimeSeriesIndex = pBuildJob.TimeSeriesIndex;
                TimeSeriesDay   = pBuildJob.TimeSeriesDay;
                Aggregates      = pBuildJob.Aggregates;
            }
        }

        class ResultsReady
        {
            public string BuildEventID   { get; private set; }
            public ulong  BuildPosition  { get; private set; }
            public string CheckpointID   { get; private set; }
            public ulong  ReadPosition   { get; private set; }
            public int    MinSeriesIndex { get; private set; }
            public int    BuildFromIndex { get; private set; }
            public int    MaxSeriesIndex { get; private set; }

            public ResultsReady(
                 Uuid  pBuildEventID
                ,ulong pBuildPosition
                ,Uuid  pCheckpointID
                ,ulong pReadPosition
                ,int   pMinSeriesIndex
                ,int   pBuildFromIndex
                ,int   pMaxSeriesIndex
            ) {
                BuildEventID   = pBuildEventID.ToString();
                BuildPosition  = pBuildPosition;
                CheckpointID   = pCheckpointID.ToString();
                ReadPosition   = pReadPosition;
                MinSeriesIndex = pMinSeriesIndex;
                BuildFromIndex = pBuildFromIndex;
                MaxSeriesIndex = pMaxSeriesIndex;
            }
        }

        public EventStoreClient Client        { get; private set; }
        public string           StreamName    { get; private set; }
        public Uuid             BuildEventID  { get; private set; }
        public StreamRevision   BuildPosition { get; private set; }

        public ResultsToEvents(EventStoreClient pClient, string pStreamName) {
            Client        = pClient;
            StreamName    = pStreamName;
            BuildPosition = new StreamRevision();
        }

        public async Task<ICommitActionResult> RegisterBuild()
        {
            try
            {
                BuildEventID = Uuid.NewUuid();

                var result = await Client.AppendToStreamAsync(
                     StreamName + "-results"
                    ,StreamState.Any
                    ,new List<EventData> { new EventData(
                         BuildEventID
                        ,"build-event"
                        ,JsonSerializer.SerializeToUtf8Bytes(
                            new BuildEvent()
                        )
                    ) }
                );

                BuildPosition = result.NextExpectedStreamRevision;
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception registering build: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        public async Task<bool> IsBuildDequeued()
        {
            var result = Client.ReadStreamAsync(
                 Direction.Backwards
                ,StreamName + "-results"
                ,StreamPosition.End
                ,1
            );

            if (await result.ReadState == ReadState.StreamNotFound) { return true; }

            var head = await result.FirstOrDefaultAsync();

            if (head.OriginalEventNumber.ToUInt64() != BuildPosition.ToUInt64())
            {
                return true;
            }

            return false;
        }

        public async Task<ICommitActionResult> CommitResultsAsync(
             List<BuildJob> pBuildQueue
            ,ulong          pReadPosition
            ,Uuid           pCheckpointID
            ,int            pMinSeriesIndex
            ,int            pBuildFromIndex
            ,int            pMaxSeriesIndex
        ) {
            if (pBuildQueue.Count == 0)
            {
                return new CommitActionResult(
                     false
                    ,"build queue was empty"
                );
            }

            var cursor = BuildPosition;

            foreach (var job in pBuildQueue)
            {
                var results = new List<EventData>();

                results.AddRange(job.ResultsDays.Select(day => {
                    return new EventData(
                         Uuid.NewUuid()
                        ,"results-day-received"
                        ,JsonSerializer.SerializeToUtf8Bytes(
                            new ResultsDayBySeries(BuildEventID, job, day)
                        )
                    );
                }).ToList());

                results.Add(
                    new EventData(
                         Uuid.NewUuid()
                        ,"aggregates-received"
                        ,JsonSerializer.SerializeToUtf8Bytes(
                            new AggregatesBySeries(BuildEventID, job)
                        )
                    )
                );

                if (job == pBuildQueue.Last())
                {
                    results.Add(
                        new EventData(
                             Uuid.NewUuid()
                            ,"results-ready"
                            ,JsonSerializer.SerializeToUtf8Bytes(
                                new ResultsReady(
                                     BuildEventID
                                    ,BuildPosition
                                    ,pCheckpointID
                                    ,pReadPosition
                                    ,pMinSeriesIndex
                                    ,pBuildFromIndex
                                    ,pMaxSeriesIndex
                                )
                            )
                        )
                    );
                }

                try
                {
                    var result = await Client.AppendToStreamAsync(
                         StreamName + "-results"
                        ,cursor
                        ,results
                    );

                    cursor = result.NextExpectedStreamRevision;
                }
                catch (Exception e)
                {
                    return new CommitActionResult(
                         false
                        ,"exception appending results to stream: " + e.Message
                    );
                }
            }

            try
            {
                await Client.AppendToStreamAsync(
                     StreamName + "-source"
                    ,StreamState.Any
                    ,new List<EventData> {
                        new EventData(
                             Uuid.NewUuid()
                            ,"checkpoint-clear"
                            ,JsonSerializer.SerializeToUtf8Bytes(
                                new CheckpointClear(pCheckpointID)
                            )
                        )
                    }
                );
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception marking checkpoint clear: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }
    }
}
