using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Client
{
   public static class TsdbFactory
   {
      public static TsdbClient CreateClient( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString )
      {
         var sql = new SqlPerformanceStorage( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage( atsTableNamme, atsConnectionString );
         var sub = new DefaultPublishSubscribe( false );
         return new TsdbClient( sql, ats, sub );
      }

      public static TsdbClient CreateClient( string sqlTableName, string sqlConnectionString, string atsTableNamme, string atsConnectionString, string redisConnectionString )
      {
         var sql = new SqlPerformanceStorage( sqlTableName, sqlConnectionString );
         var ats = new AtsVolumeStorage( atsTableNamme, atsConnectionString );
         var sub = new RedisPublishSubscribe( redisConnectionString, false );
         return new TsdbClient( sql, ats, sub );
      }

      public static TsdbEngine CreateEngine( IWorkProvider workProvider, TsdbClient client )
      {
         return new TsdbEngine( workProvider, client );
      }
   }
}
