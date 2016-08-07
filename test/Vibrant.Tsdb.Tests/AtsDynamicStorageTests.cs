using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats.Tests.Entries;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class AtsDynamicStorageTests : AbstractDynamicStorageTests<AtsDynamicStorage<BasicEntry>>
   {
      private static readonly string ConnectionString;

      static AtsDynamicStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "AtsStorage" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override AtsDynamicStorage<BasicEntry> GetStorage( string tableName )
      {
         return new AtsDynamicStorage<BasicEntry>( tableName + "Dynamic", ConnectionString );
      }
   }
}
