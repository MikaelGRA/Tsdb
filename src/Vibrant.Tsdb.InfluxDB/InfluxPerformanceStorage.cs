using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using System.Reflection;
using System.Text;

namespace Vibrant.Tsdb.InfluxDB
{
   public class InfluxPerformanceStorage<TEntry> : IPerformanceStorage<TEntry>
      where TEntry : IEntry
   {
      private InfluxClient _client;
      private string _database;

      public InfluxPerformanceStorage( Uri endpoint, string database, string username, string password )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;
      }

      public InfluxPerformanceStorage( Uri endpoint, string database )
         : this( endpoint, database, null, null )
      {

      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime to )
      {
         throw new NotImplementedException();
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         throw new NotImplementedException();
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         throw new NotImplementedException();
      }

      public Task<int> Delete( IEnumerable<string> ids )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }
   }
}
