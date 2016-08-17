using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client.Rows;

namespace Vibrant.Tsdb.InfluxDB
{
   internal class InfluxEntryAdapter<TEntry> : IInfluxRow, IHaveMeasurementName
      where TEntry : IInfluxEntry
   {
      private TEntry _entry;

      public InfluxEntryAdapter()
      {

      }

      public InfluxEntryAdapter( string measurementName, TEntry entry )
      {
         _entry = entry;
         MeasurementName = MeasurementName;
      }

      public string MeasurementName { get; set; }

      public IEnumerable<KeyValuePair<string, object>> GetAllFields()
      {
         return _entry.GetAllFields();
      }

      public IEnumerable<KeyValuePair<string, string>> GetAllTags()
      {
         return _entry.GetAllTags();
      }

      public object GetField( string name )
      {
         return _entry.GetField( name );
      }

      public string GetTag( string name )
      {
         return _entry.GetTag( name );
      }

      public DateTime? GetTimestamp()
      {
         return ( (IInfluxRow)_entry ).GetTimestamp();
      }

      public void SetField( string name, object value )
      {
         _entry.SetField( name, value );
      }

      public void SetTag( string name, string value )
      {
         _entry.SetTag( name, value );
      }

      public void SetTimestamp( DateTime? value )
      {
         ( (IInfluxRow)_entry ).SetTimestamp( value );
      }
   }
}
