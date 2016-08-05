using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Vibrant.Tsdb.Ats
{
   public class AtsDynamicStorage<TEntry> : IDynamicStorage<TEntry>, IDynamicStorageSelector<TEntry>
     where TEntry : IAtsEntry
   {
      private object _sync = new object();
      private SemaphoreSlim _getSemaphore;
      private SemaphoreSlim _setSemaphore;
      private string _tableName;
      private CloudStorageAccount _account;
      private CloudTableClient _client;
      private Task<CloudTable> _table;

      public AtsDynamicStorage( string tableName, string connectionString )
      {
         _getSemaphore = new SemaphoreSlim( 25 );
         _setSemaphore = new SemaphoreSlim( 10 );
         _tableName = tableName;
         _account = CloudStorageAccount.Parse( connectionString );
         _client = _account.CreateCloudTableClient();
      }

      public IDynamicStorage<TEntry> GetStorage( string id )
      {
         return this;
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

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         throw new NotImplementedException();
      }

      public Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         throw new NotImplementedException();
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         throw new NotImplementedException();
      }
   }
}
