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
   public class InfluxPerformanceStorage : IPerformanceStorage
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
         // cannot do this without specifying type...
         
         return (Task<MultiReadResult<IEntry>>)GetType()
            .GetMethod( "ReadLatestInternal", BindingFlags.NonPublic | BindingFlags.Instance )
            .MakeGenericMethod( typeof( IInfluxRow ) )
            .Invoke( this, new[] { ids } );
      }

      public Task Write( IEnumerable<IEntry> items )
      {
         return (Task)GetType()
            .GetMethod( "WriteInternal", BindingFlags.NonPublic | BindingFlags.Instance )
            .MakeGenericMethod( items.GetType().GetGenericArguments()[ 0 ] )
            .Invoke( this, new[] { items } );
      }

      private Task WriteInternal<TInfluxRow>( IEnumerable<TInfluxRow> items )
         where TInfluxRow : IHaveMeasurementName, IEntry, new()
      {
         return _client.WriteAsync( _database, items );
      }

      private string CreateQueries( IEnumerable<string> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( "select * from '" )
               .Append( id )
               .Append( "' limit 1" );
         }
         return sb.ToString();
      }

      private async Task<MultiReadResult<TInfluxRow>> ReadLatestInternal<TInfluxRow>( IEnumerable<string> ids )
         where TInfluxRow : IInfluxRow, IEntry, new()
      {
         var resultSet = await _client.ReadAsync<TInfluxRow>( _database, CreateQueries( ids ) ).ConfigureAwait( false );
         return CreateResult( resultSet, Sort.Descending );
      }

      private MultiReadResult<TInfluxRow> CreateResult<TInfluxRow>( InfluxResultSet<TInfluxRow> resultSet, Sort sort )
         where TInfluxRow : IInfluxRow, IEntry, new()
      {
         MultiReadResult<TInfluxRow> results = new MultiReadResult<TInfluxRow>();
         foreach( var result in resultSet.Results )
         {
            var serie = result.Series[ 0 ];
            var rows = serie.Rows;
            var read = new ReadResult<TInfluxRow>( serie.Name, sort );
            read.Entries.AddRange( rows );

            results.AddOrMerge( read );
         }
         return results;
      }
   }
}
