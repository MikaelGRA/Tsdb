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
      public static TsdbClient<TKey, TEntry> CreateSqlAtsRedisClient<TKey, TEntry>( 
         string sqlTableName, 
         string sqlConnectionString, 
         string atsTableNamme, 
         string atsConnectionString, 
         string redisConnectionString, 
         string temporaryFileDirectory,
         ITsdbLogger logger )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString );
         var sub = new RedisPublishSubscribe<TKey, TEntry>( redisConnectionString, false );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TKey, TEntry>( sql, ats, sub, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateSqlAtsRedisClient<TKey, TEntry>( 
         string sqlTableName, 
         string sqlConnectionString, 
         string atsTableNamme, 
         string atsConnectionString, 
         string redisConnectionString, 
         string temporaryFileDirectory,
         ITsdbLogger logger,
         IPartitionProvider<TKey> partitionProvider, 
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString, 5, 5, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString, 25, 25, partitionProvider, keyConverter );
         var sub = new RedisPublishSubscribe<TKey, TEntry>( redisConnectionString, false, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024, keyConverter );
         return new TsdbClient<TKey, TEntry>( sql, ats, sub, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateAtsRedisClient<TKey, TEntry>( 
         string dynamicAtsTableName, 
         string volumeAtsTableName, 
         string atsConnectionString, 
         string redisConnectionString, 
         string temporaryFileDirectory,
         ITsdbLogger logger )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new AtsDynamicStorage<TKey, TEntry>( dynamicAtsTableName, atsConnectionString );
         var ats = new AtsVolumeStorage<TKey, TEntry>( volumeAtsTableName, atsConnectionString );
         var sub = new RedisPublishSubscribe<TKey, TEntry>( redisConnectionString, false );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TKey, TEntry>( sql, ats, sub, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateAtsClient<TKey, TEntry>( 
         string dynamicAtsTableName, 
         string volumeAtsTableName, 
         string atsConnectionString, 
         string temporaryFileDirectory,
         ITsdbLogger logger )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new AtsDynamicStorage<TKey, TEntry>( dynamicAtsTableName, atsConnectionString );
         var ats = new AtsVolumeStorage<TKey, TEntry>( volumeAtsTableName, atsConnectionString );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TKey, TEntry>( sql, ats, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateAtsClient<TKey, TEntry>( 
         string dynamicAtsTableName,
         string volumeAtsTableName,
         string atsConnectionString, 
         string temporaryFileDirectory,
         ITsdbLogger logger,
         IPartitionProvider<TKey> dynamicPartitionProvider, 
         IPartitionProvider<TKey> volumePartitionProvider, 
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new AtsDynamicStorage<TKey, TEntry>( dynamicAtsTableName, atsConnectionString, 25, 25, dynamicPartitionProvider, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( volumeAtsTableName, atsConnectionString, 25, 25, volumePartitionProvider, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024, keyConverter );
         return new TsdbClient<TKey, TEntry>( sql, ats, files, logger );
      }
      public static TsdbClient<TKey, TEntry> CreateSqlAtsClient<TKey, TEntry>(
         string sqlTableName,
         string sqlConnectionString,
         string atsTableNamme,
         string atsConnectionString,
         string temporaryFileDirectory,
         ITsdbLogger logger )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024 );
         return new TsdbClient<TKey, TEntry>( sql, ats, null, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateSqlAtsClient<TKey, TEntry>(
         string sqlTableName,
         string sqlConnectionString,
         string atsTableNamme,
         string atsConnectionString,
         string temporaryFileDirectory,
         ITsdbLogger logger,
         IPartitionProvider<TKey> partitionProvider,
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString, 5, 5, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString, 25, 25, partitionProvider, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, 1 * 1024 * 1024, 1024 * 1024 * 1024, keyConverter );
         return new TsdbClient<TKey, TEntry>( sql, ats, null, files, logger );
      }
   }
}
