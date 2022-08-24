using System;
using System.Configuration;
using System.Data;
using Uniformance.PHD;

namespace PHDService
{
    public class UniformancePHD
    {
        private const string PHD_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
        public DataSet FetchRawDataset(string tagName, DateTime dtStartDtTime, DateTime dtEndDtTime, Int32 iMinConf)
        {

            Double[] timestamps = null;
            Double[] values = null;
            short[] confidences = null;

            PHDHistorian phd = new PHDHistorian();
            phd.DefaultServer = new PHDServer(ConfigurationManager.AppSettings["PHD_HostName"], SERVERVERSION.API200);
            phd.DefaultServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["PHD_Port"]);
            phd.Sampletype = SAMPLETYPE.Raw;
            phd.ReductionType = REDUCTIONTYPE.None;

            phd.MinimumConfidence = iMinConf;
            Tag selectedTag = new Tag(tagName);
            phd.StartTime = dtStartDtTime.ToString(PHD_DATETIME_FORMAT);
            phd.EndTime = dtEndDtTime.ToString(PHD_DATETIME_FORMAT);

            phd.FetchData(selectedTag, ref timestamps, ref values, ref confidences);
            int count = timestamps.GetUpperBound(0);

            phd.Dispose();

            return ExportToDataset(timestamps, values, confidences);
        }

        public DataSet FetchSimpleSnapshotDataset(string tagName, DateTime startTime, DateTime endTime, Int32 minConfidence)
        {
            Double[] timestamps = null;
            Double[] values = null;
            short[] confidences = null;

            PHDHistorian phd = new PHDHistorian();
            phd.DefaultServer = new PHDServer(ConfigurationManager.AppSettings["PHD_HostName"], SERVERVERSION.API200);
            phd.DefaultServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["PHD_Port"]);

            phd.SampleFrequency = 60;
            phd.Sampletype = SAMPLETYPE.Snapshot;
            phd.UseSampleFrequency = false;
            phd.ReductionOffset = REDUCTIONOFFSET.Around;

            phd.MinimumConfidence = minConfidence;
            Tag selectedTag = new Tag(tagName);
            phd.StartTime = startTime.ToString(PHD_DATETIME_FORMAT);
            phd.EndTime = endTime.ToString(PHD_DATETIME_FORMAT);

            phd.FetchData(selectedTag, ref timestamps, ref values, ref confidences);
            int count = timestamps.GetUpperBound(0);


            return ExportToDataset(timestamps, values, confidences);
        }

        public DataSet FetchDataPHDDataset(string tagName, DateTime startTime, DateTime endTime, Int32 minConfidence, string strAggregate, uint frequency)
        {

            Double[] timestamps = null;
            Double[] values = null;
            short[] confidences = null;

            PHDHistorian phd = new PHDHistorian();
            phd.DefaultServer = new PHDServer(ConfigurationManager.AppSettings["PHD_HostName"], SERVERVERSION.API200);
            phd.DefaultServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["PHD_Port"]);

            phd.SampleFrequency = frequency;
            if (strAggregate.ToUpper().Equals("AVERAGE"))
            {
                phd.Sampletype = SAMPLETYPE.Average;
            }
            else
            if (strAggregate.ToUpper().Equals("INTERPOLATEDRAW"))
            {
                phd.Sampletype = SAMPLETYPE.InterpolatedRaw;
            }
            else
            if (strAggregate.ToUpper().Equals("RAW"))
            {
                phd.Sampletype = SAMPLETYPE.Raw;
            }
            else
            if (strAggregate.ToUpper().Equals("RESAMPLED"))
            {
                phd.Sampletype = SAMPLETYPE.Resampled;
            }
            else
            if (strAggregate.ToUpper().Equals("SNAPSHOT"))
            {
                phd.Sampletype = SAMPLETYPE.Snapshot;
            }
            else
            {
                throw new Exception("Please Enter Valid Aggregate Methods");
            }
            phd.MinimumConfidence = minConfidence;
            Tag selectedTag = new Tag(tagName);
            phd.StartTime = startTime.ToString(PHD_DATETIME_FORMAT);
            phd.EndTime = endTime.ToString(PHD_DATETIME_FORMAT);

            phd.FetchData(selectedTag, ref timestamps, ref values, ref confidences);
            int count = timestamps.GetUpperBound(0);

            phd.Dispose();

            return ExportToDataset(timestamps, values, confidences);
        }

        private DataSet ExportToDataset(Double[] timestamps, Double[] values, short[] confidences)
        {
            DataSet ds = new DataSet();
            DataTable DT = new DataTable();
            DataRow dr;
            int i;
            ds.Tables.Add(DT);

            ds.Tables[0].Columns.Add("Timestamp", typeof(string));
            ds.Tables[0].Columns.Add("Data", typeof(decimal));
            ds.Tables[0].Columns.Add("Confidence", typeof(decimal));


            for (i = 0; i <= values.Length - 1; i++)
            {
                if (Convert.ToDecimal(values[i]) != null)
                {
                    dr = ds.Tables[0].NewRow();
                    dr["Timestamp"] = DateTime.FromOADate(timestamps[i]).ToString("MM/dd/yyyy HH:mm:ss tt");
                    dr["Data"] = Convert.ToDecimal(values[i]);
                    dr["Confidence"] = Convert.ToDecimal(confidences[i]);
                    ds.Tables[0].Rows.Add(dr);
                }
            }

            return ds;
        }


        public bool RemPHD(string tagName, DateTime setTime)
        {

            bool blStatus = false;
            try
            {
                PHDHistorian phd = new PHDHistorian();
                phd.DefaultServer = new PHDServer(ConfigurationManager.AppSettings["PHD_HostName"], SERVERVERSION.API200);
                phd.DefaultServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["PHD_Port"]);
                Tag selectedTag = new Tag(tagName);
                phd.DeleteTagData(selectedTag, setTime.ToString(PHD_DATETIME_FORMAT));
                blStatus = true;


            }
            catch (Exception ex)
            {
                blStatus = false;
            }

            return blStatus;

        }

        public bool SetPHD(string tagName, DateTime setTime, Int32 minConfidence, double RecValue)
        {

            bool blStatus = false;
            try
            {
                PHDHistorian phd = new PHDHistorian();
                phd.DefaultServer = new PHDServer(ConfigurationManager.AppSettings["PHD_HostName"], SERVERVERSION.API200);
                phd.DefaultServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["PHD_Port"]);
                Tag selectedTag = new Tag(tagName);
                phd.ModifyTag(selectedTag, RecValue, setTime.ToString(PHD_DATETIME_FORMAT), Convert.ToSByte(minConfidence));
                blStatus = true;

            }
            catch (Exception e)
            {
                blStatus = false;
            }

            return blStatus;
        }


    }
}