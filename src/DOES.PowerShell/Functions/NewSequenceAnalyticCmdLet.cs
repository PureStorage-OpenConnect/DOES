using System;
using System.Management.Automation;
using DOES.Shared.Debug;
using DOES.Shared.Operations;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsCommon.New, "SequenceAnalytic")]
    public class NewSequenceAnalyticCmdLet : Cmdlet
    {
        //Mandatory Parameters 
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true)]
        public string TestName { get; set; }
        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true)]
        public string ObjectName { get; set; }
        [Parameter(Position = 2, Mandatory = true, ValueFromPipeline = true)]
        public int Sequence { get; set; }
        [Parameter(Position = 3)]
        public SwitchParameter LogData
        {
            get { return logDataCheck; }
            set { logDataCheck = value; }
        }
        private bool logDataCheck;

        //DataParameters
        [Parameter(Mandatory = false)]
        public DateTime? DataImportStart { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? DataImportEnd { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? DataChangeStart { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? DataChangeEnd { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? CoreJobStart { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? CoreJobEnd { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? OtherJobStart { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? OtherJobEnd { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? SequenceStart { get; set; }
        [Parameter(Mandatory = false)]
        public DateTime? SequenceEnd { get; set; }
        private MessageQueue _messageQueue;


        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void ProcessRecord()
        {
            _messageQueue = new MessageQueue();

            if (logDataCheck)
            {
                Analytic analytic = new Analytic(TestName, ObjectName, Sequence, _messageQueue);

                Sequence seq = new Sequence(analytic, Sequence);
                string columnname = null;
                if (DataImportStart.HasValue)
                {
                    columnname = "DataImportStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(DataImportStart), seq);
                }
                if (DataImportEnd.HasValue)
                {
                    columnname = "DataImportEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(DataImportEnd), seq);
                }
                if (DataChangeStart.HasValue)
                {
                    columnname = "DataChangeStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(DataChangeStart), seq);
                }
                if (DataChangeEnd.HasValue)
                {
                    columnname = "DataChangeEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(DataChangeEnd), seq);
                }
                if (CoreJobStart.HasValue)
                {
                    columnname = "CoreJobStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(CoreJobStart), seq);
                }
                if (CoreJobEnd.HasValue)
                {
                    columnname = "CoreJobEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(CoreJobEnd), seq);
                }
                if (OtherJobStart.HasValue)
                {
                    columnname = "OtherJobStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(OtherJobStart), seq);
                }
                if (OtherJobEnd.HasValue)
                {
                    columnname = "OtherJobEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(OtherJobEnd), seq);
                }
                if (SequenceStart.HasValue)
                {
                    columnname = "SequenceStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(SequenceStart), seq);
                }
                if (SequenceEnd.HasValue)
                {
                    columnname = "SequenceEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(SequenceEnd), seq);
                }
            }
        }

        protected override void StopProcessing()
        {
            base.StopProcessing();
        }
    }
}
