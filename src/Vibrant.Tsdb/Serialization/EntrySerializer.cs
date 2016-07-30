using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Serialization
{
   public static class EntrySerializer
   {
      public static BinaryReader CreateReader( Stream stream )
      {
         var reader = new BinaryReader( stream, Encoding.ASCII );
         return reader;
      }

      public static BinaryWriter CreateWriter( Stream stream )
      {
         var writer = new BinaryWriter( stream, Encoding.ASCII );
         return writer;
      }

      public static void SerializeEntry( BinaryWriter writer, IEntry entry )
      {
         writer.Write( entry.GetTypeCode() );
         entry.Write( writer );
      }

      public static IEntry DeserializeEntry( string id, BinaryReader reader )
      {
         var typeCode = reader.ReadByte();
         var entry = TsdbTypeRegistry.CreateEntry( typeCode );
         entry.SetId( id );
         entry.Read( reader );
         return entry;
      }
   }
}
