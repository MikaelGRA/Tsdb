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
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory );
         return new TsdbClient<TKey, TEntry>( sql, ats, sub, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateSqlAtsRedisClient<TKey, TEntry>( 
         string sqlTableName, 
         string sqlConnectionString, 
         string atsTableNamme, 
         string atsConnectionString, 
         string redisConnectionString, 
         string temporaryFileDirectory,
         int sqlReadParallelism,
         int sqlWriteParallelism,
         int atsReadParallelism,
         int atsWriteParallelism,
         int maxTemporaryFileSize,
         int maxTemporaryStorageSize,
         ITsdbLogger logger,
         IPartitionProvider<TKey> partitionProvider, 
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString, sqlReadParallelism, sqlWriteParallelism, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString, atsReadParallelism, atsWriteParallelism, partitionProvider, keyConverter );
         var sub = new RedisPublishSubscribe<TKey, TEntry>( redisConnectionString, false, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, maxTemporaryFileSize, maxTemporaryStorageSize, keyConverter );
         return new TsdbClient<TKey, TEntry>( sql, ats, sub, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateAtsRedisClient<TKey, TEntry>( 
         string dynamicAtsTableName, 
         string volumeAtsTableName, 
         string atsConnectionString, 
         string redisConnectionString, 
         string temporaryFileDirectory,
         int maxTemporaryFileSize,
         int maxTemporaryStorageSize,
         ITsdbLogger logger )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new AtsDynamicStorage<TKey, TEntry>( dynamicAtsTableName, atsConnectionString );
         var ats = new AtsVolumeStorage<TKey, TEntry>( volumeAtsTableName, atsConnectionString );
         var sub = new RedisPublishSubscribe<TKey, TEntry>( redisConnectionString, false );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, maxTemporaryFileSize, maxTemporaryStorageSize );
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
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory );
         return new TsdbClient<TKey, TEntry>( sql, ats, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateAtsClient<TKey, TEntry>( 
         string dynamicAtsTableName,
         string volumeAtsTableName,
         string atsConnectionString, 
         string temporaryFileDirectory,
         int dynamicAtsReadParallelism,
         int dynamicAtsWriteParallelism,
         int volumeAtsReadParallelism,
         int volumeAtsWriteParallelism,
         int maxTemporaryFileSize,
         int maxTemporaryStorageSize,
         ITsdbLogger logger,
         IPartitionProvider<TKey> dynamicPartitionProvider, 
         IPartitionProvider<TKey> volumePartitionProvider, 
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new AtsDynamicStorage<TKey, TEntry>( dynamicAtsTableName, atsConnectionString, dynamicAtsReadParallelism, dynamicAtsWriteParallelism, dynamicPartitionProvider, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( volumeAtsTableName, atsConnectionString, volumeAtsReadParallelism, volumeAtsWriteParallelism, volumePartitionProvider, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, maxTemporaryFileSize, maxTemporaryStorageSize, keyConverter );
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
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory );
         return new TsdbClient<TKey, TEntry>( sql, ats, null, files, logger );
      }

      public static TsdbClient<TKey, TEntry> CreateSqlAtsClient<TKey, TEntry>(
         string sqlTableName,
         string sqlConnectionString,
         string atsTableNamme,
         string atsConnectionString,
         string temporaryFileDirectory,
         int sqlReadParallelism,
         int sqlWriteParallelism,
         int atsReadParallelism,
         int atsWriteParallelism,
         int maxTemporaryFileSize,
         int maxTemporaryStorageSize,
         ITsdbLogger logger,
         IPartitionProvider<TKey> partitionProvider,
         IKeyConverter<TKey> keyConverter )
         where TEntry : IEntry<TKey>, IAtsEntry<TKey>, ISqlEntry<TKey>, IRedisEntry<TKey>, IFileEntry<TKey>, new()
      {
         var sql = new SqlDynamicStorage<TKey, TEntry>( sqlTableName, sqlConnectionString, sqlReadParallelism, sqlWriteParallelism, keyConverter );
         var ats = new AtsVolumeStorage<TKey, TEntry>( atsTableNamme, atsConnectionString, atsReadParallelism, atsWriteParallelism, partitionProvider, keyConverter );
         var files = new TemporaryFileStorage<TKey, TEntry>( temporaryFileDirectory, maxTemporaryFileSize, maxTemporaryStorageSize, keyConverter );
         return new TsdbClient<TKey, TEntry>( sql, ats, null, files, logger );
      }
   }
}
