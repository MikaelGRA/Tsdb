using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Vibrant.Tsdb.Ats.Tests.Selectors;
using Xunit;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class MigrationProviderTests
   {

      private static MigrationProvider<string, BasicEntry> CreateMigrationProvider()
      {
         var dynamics = new MyDynamicStorageSelector<string, BasicEntry>(
            new[]
            {
               new StorageSelection<string, BasicEntry, IDynamicStorage<string, BasicEntry>>( null, new DateTime( 2017, 1, 1 ), new DateTime( 2020, 1, 1 ) ),
               new StorageSelection<string, BasicEntry, IDynamicStorage<string, BasicEntry>>( null, new DateTime( 2011, 1, 1 ), new DateTime( 2014, 1, 1 ) ),
            } );

         var volumes = new MyVolumeStorageSelector<string, BasicEntry>(
            new[]
            {
               new StorageSelection<string, BasicEntry, IVolumeStorage<string, BasicEntry>>( null, new DateTime( 2010, 1, 1 ), new DateTime( 2021, 1, 1 ) ),
            } );

         var provider = new MigrationProvider<string, BasicEntry>( dynamics, volumes );

         return provider;
      }

      [Theory( DisplayName = "Should_Select_Correct_Storages" )]
      [InlineData( null, null, 2 )]
      [InlineData( 2010, 2021, 2 )]
      [InlineData(2009, 2010, 0)]
      [InlineData(2011, 2012, 1)]
      [InlineData(2012, 2013, 1)]
      [InlineData(2013, 2014, 1)]
      [InlineData(2014, 2015, 0)]
      [InlineData(2014, 2017, 0)]
      [InlineData(2015, 2016, 0)]
      [InlineData(2016, 2017, 0)]
      [InlineData(2017, 2018, 1)]
      [InlineData(2020, 2021, 0)]
      public void Should_Select_Correct_Storages(int? fromYear, int? toYear, int expected)
      {
         var provider = CreateMigrationProvider();

         DateTime? from = null;
         if( fromYear.HasValue )
         {
            from = new DateTime( fromYear.Value, 1, 1 );
         }

         DateTime? to = null;
         if( toYear.HasValue )
         {
            to = new DateTime( toYear.Value, 1, 1 );
         }

         var migrations = provider.Provide( "lol", from, to ).ToList();
         Assert.Equal( expected, migrations.Count );
         //Assert.Equal( new DateTime( 2012, 1, 1 ), migrations[ 0 ].From );
         //Assert.Equal( new DateTime( 2013, 1, 1 ), migrations[ 0 ].To );
         //Assert.Equal( new DateTime( 2011, 1, 1 ), migrations[ 1 ].From );
         //Assert.Equal( new DateTime( 2012, 1, 1 ), migrations[ 1 ].To );
      }
   }
}
