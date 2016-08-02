using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Serialization;

namespace Vibrant.Tsdb.Ats.Serialization
{
   internal static class AtsSerializer
   {
      public static List<AtsSerializationResult> Serialize( List<IEntry> entries, int maxByteArraySize )
      {
         var results = new List<AtsSerializationResult>();
         var stream = new MemoryStream();
         var writer = EntrySerializer.CreateWriter( stream );
         DateTime? from = null;
         int batchYear = 0;

         int currentSize = 0;
         List<byte[]> serializedEntries = new List<byte[]>();

         for( int i = entries.Count - 1 ; i > -1 ; i-- )
         {
            var entry = entries[ i ];

            var timestamp = entry.GetTimestamp();
            if( !from.HasValue )
            {
               from = timestamp;
               batchYear = timestamp.Year;
            }
            else
            {
               int otherEntryYear = timestamp.Year;
               if( batchYear != otherEntryYear )
               {
                  writer.Write( serializedEntries.Count );
                  var length = stream.ToArray();

                  // reset stream
                  stream.Seek( 0, SeekOrigin.Begin );
                  stream.SetLength( 0 );

                  // create big array from mall the small arrays
                  var data = CreateData( currentSize, length, serializedEntries );

                  // add the created result
                  results.Add( new AtsSerializationResult( from.Value, data ) );

                  // reset parameters
                  from = null;
                  currentSize = 0;
                  serializedEntries = new List<byte[]>( serializedEntries.Count );
               }
            }

            if( currentSize + TsdbTypeRegistry.MaxEntrySizeInBytes > maxByteArraySize )
            {
               writer.Write( serializedEntries.Count );
               var length = stream.ToArray();

               // reset stream
               stream.Seek( 0, SeekOrigin.Begin );
               stream.SetLength( 0 );

               // create big array from mall the small arrays
               var data = CreateData( currentSize, length, serializedEntries );

               // add the created result
               results.Add( new AtsSerializationResult( from.Value, data ) );

               // reset parameters
               from = null;
               currentSize = 0;
               serializedEntries = new List<byte[]>( serializedEntries.Count );
            }

            EntrySerializer.SerializeEntry( writer, entry, true, false );
            writer.Flush(); // is this needed for a memory stream????

            var serializedEntry = stream.ToArray();
            serializedEntries.Add( serializedEntry );

            // update parameters
            currentSize += serializedEntry.Length;

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         if( currentSize > 0 )
         {
            writer.Write( serializedEntries.Count );
            var length = stream.ToArray();

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );

            var data = CreateData( currentSize, length, serializedEntries );

            results.Add( new AtsSerializationResult( from.Value, data ) );
         }

         writer.Dispose();

         return results;
      }

      private static byte[] CreateData( int size, byte[] length, List<byte[]> serializedEntries )
      {
         var data = new byte[ size + length.Length ];
         Buffer.BlockCopy( length, 0, data, 0, length.Length );

         var copied = length.Length;
         for( int j = serializedEntries.Count - 1 ; j > -1 ; j-- )
         {
            var serializedEntry = serializedEntries[ j ];
            Buffer.BlockCopy( serializedEntry, 0, data, copied, serializedEntry.Length );
            copied += serializedEntry.Length;
         }
         return data;
      }

      public static IEntry[] Deserialize( string id, byte[] bytes, Sort sort )
      {
         var stream = new MemoryStream( bytes );
         var reader = EntrySerializer.CreateReader( stream );
         var count = reader.ReadInt32();
         IEntry[] entries = new IEntry[ count ];

         if( sort == Sort.Ascending )
         {
            int idx = count;
            while( stream.Length != stream.Position )
            {
               var entry = EntrySerializer.DeserializeEntry( reader, id, true );
               entries[ --idx ] = entry;
            }
         }
         else
         {
            int idx = 0;
            while( stream.Length != stream.Position )
            {
               var entry = EntrySerializer.DeserializeEntry( reader, id, true );
               entries[ idx++ ] = entry;
            }
         }

         return entries;
      }
   }
}
