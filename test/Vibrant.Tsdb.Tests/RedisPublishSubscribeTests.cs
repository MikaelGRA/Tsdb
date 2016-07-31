﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Redis;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class RedisPublishSubscribeTests : AbstractPublishSubscribeTests<RedisPublishSubscribe>
   {
      private static readonly string ConnectionString;

      static RedisPublishSubscribeTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "RedisCache" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override RedisPublishSubscribe CreatePublishSubscribe()
      {
         return new RedisPublishSubscribe( ConnectionString, false );
      }
   }
}
