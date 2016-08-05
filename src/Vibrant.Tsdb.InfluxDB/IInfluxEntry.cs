using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client.Rows;

namespace Vibrant.Tsdb.InfluxDB
{
   public interface IInfluxEntry : IInfluxRow, IHaveMeasurementName
   {
   }
}
