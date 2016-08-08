using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Files;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Client
{
   /// <summary>
   /// Factory class that allows easy instantiation of TsdbClient and TsdbEngine.
   /// </summary>
   public static class TsdbFactory
   {
      public static TsdbClient<TEntry> CreateClient<TEntry>( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString, string temporaryFileDirectory )
         where TEntry : IEntry, IAtsEntry, ISqlEntry, IFileEntry, new()
      {
         var sql = new SqlDynamicStorage<TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TEntry>( atsTableNamme, atsConnectionString );
         var sub = new DefaultPublishSubscribe<TEntry>( false );
         var files = new TemporaryFileStorage<TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TEntry>( sql, ats, sub, files );
      }

      public static TsdbClient<TEntry> CreateClient<TEntry>( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString, string temporaryFileDirectory, string redisConnectionString )
         where TEntry : IEntry, IAtsEntry, ISqlEntry, IFileEntry, IRedisEntry, new()
      {
         var sql = new SqlDynamicStorage<TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TEntry>( atsTableNamme, atsConnectionString );
         var sub = new RedisPublishSubscribe<TEntry>( redisConnectionString, false );
         var files = new TemporaryFileStorage<TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TEntry>( sql, ats, sub, files );
      }

      public static TsdbEngine<TEntry> CreateEngine<TEntry>( IWorkProvider workProvider, TsdbClient<TEntry> client )
         where TEntry : IEntry
      {
         return new TsdbEngine<TEntry>( workProvider, client );
      }
   }
}
