using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Vibrant.Tsdb.Files;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class TemporaryFileStorageTests : AbstractTemporaryStorageTests<TemporaryFileStorage<BasicEntry>>
   {
      private static readonly string ConnectionString;

      static TemporaryFileStorageTests()
      {
      }

      public override TemporaryFileStorage<BasicEntry> GetStorage()
      {
         return new TemporaryFileStorage<BasicEntry>( @"c:\temp\lol\test", 64 * 1024, 1024 * 1024 * 1024 );
      }
   }
}
