using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client.Rows;

namespace Vibrant.Tsdb.InfluxDB
{
   internal class TypedInfluxEntryAdapter<TKey, TEntry, TMeasureType> : IInfluxRow, IHaveMeasurementName
      where TEntry : IInfluxEntry
      where TMeasureType : IMeasureType
   {
      private Dictionary<string, string> _additionalTags;
      private TEntry _entry;
      private string _uidTag;
      private string _measurementnName;

      public TypedInfluxEntryAdapter()
      {

      }

      public TypedInfluxEntryAdapter( string measurementName, string uidTag, Dictionary<string, string> additionalTags, TEntry entry )
      {
         _measurementnName = measurementName;
         _uidTag = uidTag;
         _additionalTags = additionalTags;
         _entry = entry;
      }

      public string MeasurementName
      {
         get
         {
            return _measurementnName;
         }
         set
         {
            throw new NotSupportedException();
         }
      }

      public IEnumerable<KeyValuePair<string, object>> GetAllFields()
      {
         return _entry.GetAllFields();
      }

      public IEnumerable<KeyValuePair<string, string>> GetAllTags()
      {
         yield return new KeyValuePair<string, string>( ReservedNames.UniqueId, _uidTag );
         foreach( var kvp in _additionalTags )
         {
            yield return kvp;
         }
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
