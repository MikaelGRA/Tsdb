using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Tests.Entries;

namespace Vibrant.Tsdb.Tests
{
   public class AtsVolumeStorageTests : AbstractStorageTests<AtsVolumeStorage<string, BasicEntry>>
   {
      private static readonly string ConnectionString;

      static AtsVolumeStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "AtsStorage" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override AtsVolumeStorage<string, BasicEntry> GetStorage( string tableName )
      {
         return new AtsVolumeStorage<string, BasicEntry>( tableName, ConnectionString );
      }
   }
}
