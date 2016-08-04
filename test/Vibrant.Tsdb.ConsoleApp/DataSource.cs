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

      public DataSource( string id, DateTime startTime, TimeSpan interval )
      {
         Id = id;
         _currentTimestamp = startTime;
         Interval = interval;
      }

      public string Id { get; private set; }

      public TimeSpan Interval { get; private set; }

      public IEnumerable<BasicEntry> GetEntries( DateTime now )
      {
         List<BasicEntry> entries = new List<BasicEntry>();

         for( var timestamp = _currentTimestamp ; timestamp < now ; timestamp += Interval )
         {
            var entry = new BasicEntry();
            entry.Id = Id;
            entry.Timestamp = timestamp;

            _velocity = _rng.NextDouble() - 0.5;
            _currentValue += _velocity;

            entry.Value = _currentValue;

            entries.Add( entry );
         }
         _currentTimestamp = now;

         return entries;
      }
   }
}
