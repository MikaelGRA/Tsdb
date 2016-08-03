using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;

namespace Vibrant.Tsdb.Ats.Tests.Entries
{
   public class BasicEntry : IEntry, IInfluxRow, IHaveMeasurementName
   {
      private KeyValuePair<string, string>[] _empty = new KeyValuePair<string, string>[ 0 ];

      public string Id { get; set; }

      public DateTime Timestamp { get; set; }

      public double Value { get; set; }

      public string MeasurementName
      {
         get
         {
            return Id;
         }
         set
         {
            Id = value;
         }
      }

      public string GetId()
      {
         return Id;
      }

      public void SetId( string id )
      {
         Id = id;
      }

      public DateTime GetTimestamp()
      {
         return Timestamp;
      }

      public void SetTimestamp( DateTime timestamp )
      {
         Timestamp = timestamp;
      }

      public ushort GetTypeCode()
      {
         return 1;
      }

      public void Read( BinaryReader reader )
      {
         Value = reader.ReadDouble();
      }

      public void Write( BinaryWriter writer )
      {
         writer.Write( Value );
      }

      void IInfluxRow.SetTimestamp( DateTime? value )
      {
         Timestamp = value.Value;
      }

      DateTime? IInfluxRow.GetTimestamp()
      {
         return Timestamp;
      }

      void IInfluxRow.SetField( string name, object value )
      {
         switch( name )
         {
            case "Value":
               Value = (double)value;
               break;
            default:
               throw new ArgumentException( "name" );
         }
      }

      object IInfluxRow.GetField( string name )
      {
         switch( name )
         {
            case "Value":
               return Value;
            default:
               throw new ArgumentException( "name" );
         }
      }

      void IInfluxRow.SetTag( string name, string value )
      {
         throw new NotImplementedException();
      }

      string IInfluxRow.GetTag( string name )
      {
         throw new NotImplementedException();
      }

      IEnumerable<KeyValuePair<string, string>> IInfluxRow.GetAllTags()
      {
         return _empty;
      }

      IEnumerable<KeyValuePair<string, object>> IInfluxRow.GetAllFields()
      {
         yield return new KeyValuePair<string, object>( "Value", Value );
      }
   }
}
