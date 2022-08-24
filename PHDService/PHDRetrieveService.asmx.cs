using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Web.Services.Protocols;
using Uniformance.PHD;

namespace PHDService
{
    /// <summary>
    /// Summary description for PHDRetrieveService
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/PhdService/Service1")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class PHDRetrieveService : System.Web.Services.WebService
    {

        [WebMethod()]
        public DataSet GetRawPHDReading(string dtStartDtTime, string dtEndDtTime, Int32 iMinConf, string strTagId)
        {
            UniformancePHD uniformancePHD = new UniformancePHD();
            return uniformancePHD.FetchRawDataset(strTagId, Convert.ToDateTime(dtStartDtTime), Convert.ToDateTime(dtEndDtTime), iMinConf);

        }

        [WebMethod()]
        public DataSet GetSimpleSnapshotPHDReading(string dtStartDtTime, string dtEndDtTime, Int32 iMinConf, string strTagId)
        {
            UniformancePHD uniformancePHD = new UniformancePHD();
            return uniformancePHD.FetchSimpleSnapshotDataset(strTagId, Convert.ToDateTime(dtStartDtTime), Convert.ToDateTime(dtEndDtTime), iMinConf);

        }

        [WebMethod()]
        public DataSet GetDataPHDReading(string dtStartDtTime, string dtEndDtTime, Int32 iMinConf, string strTagId, string strAggregate, uint interval)
        {
            try
            {
                if (interval <= 0)
                {
                    throw new Exception("Interval has to greater than 0 seconds");
                }
                UniformancePHD uniformancePHD = new UniformancePHD();
                return uniformancePHD.FetchDataPHDDataset(strTagId, Convert.ToDateTime(dtStartDtTime), Convert.ToDateTime(dtEndDtTime), iMinConf, strAggregate, interval);

            }
            catch (Exception e)
            {
                throw e;
            }


        }


        [WebMethod()]
        public bool RemPHD(string strTagId, string dtDtTime)
        {
            string strAllowedTagUpdateList;
            AppSettingsReader appSettings = new AppSettingsReader();
            strAllowedTagUpdateList = (string)appSettings.GetValue("TagUpdate", typeof(string));
            if (strAllowedTagUpdateList.ToLower().Contains(strTagId.ToLower()))
            {
                UniformancePHD uniformancePHD = new UniformancePHD();
                return uniformancePHD.RemPHD(strTagId, Convert.ToDateTime(dtDtTime));
            }

            else
            {
                return false;
            }

        }
        [WebMethod()]
        public bool SetPHD(string strTagId, string dtDtTime, Int32 iMinConf, double RecValue)
        {
            string strAllowedTagUpdateList;
            AppSettingsReader appSettings = new AppSettingsReader();
            strAllowedTagUpdateList = (string)appSettings.GetValue("TagUpdate", typeof(string));
            if (strAllowedTagUpdateList.ToLower().Contains(strTagId.ToLower()))
            {
                UniformancePHD uniformancePHD = new UniformancePHD();
                return uniformancePHD.SetPHD(strTagId, Convert.ToDateTime(dtDtTime), iMinConf, RecValue);


            }
            else
            {
                return false;
            }

        }






    }




}

