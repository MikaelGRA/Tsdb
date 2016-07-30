using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class AtsStorageTests : AbstractStorageTests<AtsVolumeStorage>
   {
      private static readonly string ConnectionString;

      static AtsStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "AtsStorage" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override IStorage GetStorage( string tableName )
      {
         return new AtsVolumeStorage( tableName, ConnectionString );
      }
   }
}
