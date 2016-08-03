using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;

namespace Vibrant.Tsdb.InfluxDB
{
   public class InfluxPerformanceStorage : IPerformanceStorage
   {
      private InfluxClient _client;

      public InfluxPerformanceStorage( Uri endpoint, string username, string password )
      {
         _client = new InfluxClient( endpoint, username, password );
      }

      public Task<int> Delete( IEnumerable<string> ids )
      {
         throw new NotImplementedException();
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime to )
      {
         throw new NotImplementedException();
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<IEntry>> ReadLatest( IEnumerable<string> ids )
      {
         throw new NotImplementedException();
      }

      public Task Write( IEnumerable<IEntry> items )
      {
         throw new NotImplementedException();
      }
   }
}
