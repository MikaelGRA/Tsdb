using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Tests.Entries;

namespace Vibrant.Tsdb.Tests
{
   public class AtsStorageTests : AbstractStorageTests<AtsStorage<string, BasicEntry>>
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

      public override AtsStorage<string, BasicEntry> GetStorage( string tableName )
      {
         return new AtsStorage<string, BasicEntry>( tableName + "Dynamic", ConnectionString );
      }
   }
}
