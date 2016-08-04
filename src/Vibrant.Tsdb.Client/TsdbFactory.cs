using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Client
{
   /// <summary>
   /// Factory class that allows easy instantiation of TsdbClient and TsdbEngine.
   /// </summary>
   public static class TsdbFactory
   {
      public static TsdbClient<TEntry> CreateClient<TEntry>( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString )
         where TEntry : IEntry, IAtsEntry, ISqlEntry
      {
         var sql = new SqlPerformanceStorage<TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TEntry>( atsTableNamme, atsConnectionString );
         var sub = new DefaultPublishSubscribe<TEntry>( false );
         return new TsdbClient<TEntry>( sql, ats, sub );
      }

      public static TsdbClient<TEntry> CreateClient<TEntry>( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString, string redisConnectionString )
         where TEntry : IEntry, IAtsEntry, ISqlEntry, IRedisEntry
      {
         var sql = new SqlPerformanceStorage<TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TEntry>( atsTableNamme, atsConnectionString );
         var sub = new RedisPublishSubscribe<TEntry>( redisConnectionString, false );
         return new TsdbClient<TEntry>( sql, ats, sub );
      }

      public static TsdbEngine<TEntry> CreateEngine<TEntry>( IWorkProvider workProvider, TsdbClient<TEntry> client )
         where TEntry : IEntry
      {
         return new TsdbEngine<TEntry>( workProvider, client );
      }
   }
}
