using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.ConsoleApp.Entries;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class DataSource
   {
      private static readonly Random _rng = new Random();

      private double _currentValue;
      private double _velocity = 0;
      private DateTime _currentTimestamp;

      public DataSource( BasicKey id, DateTime startTime, TimeSpan interval )
      {
         Id = id;
         _currentTimestamp = startTime;
         Interval = interval;
      }

      public BasicKey Id { get; private set; }

      public TimeSpan Interval { get; private set; }

      public Serie<BasicKey, BasicEntry> GetEntries( DateTime now )
      {
         var serie = new Serie<BasicKey, BasicEntry>( Id );

         for( var timestamp = _currentTimestamp ; timestamp < now ; timestamp += Interval )
         {
            var entry = new BasicEntry();
            entry.Timestamp = timestamp;

            _velocity = _rng.NextDouble() - 0.5;
            _currentValue += _velocity;

            entry.Value = _currentValue;

            serie.Entries.Add( entry );
         }
         _currentTimestamp = now;

         return serie;
      }
   }
}
