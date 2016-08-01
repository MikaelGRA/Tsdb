﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Serialization;

namespace Vibrant.Tsdb.Ats.Serialization
{
   internal static class RedisSerializer
   {
      public static List<byte[]> Serialize( List<IEntry> entries, int maxByteArraySize )
      {
         var results = new List<byte[]>();
         var stream = new MemoryStream();
         var writer = EntrySerializer.CreateWriter( stream );

         int currentSize = 0;
         List<byte[]> serializedEntries = new List<byte[]>();

         for( int i = entries.Count - 1 ; i > -1 ; i-- )
         {
            var entry = entries[ i ];

            if( currentSize + TsdbTypeRegistry.MaxEntrySizeInBytes > maxByteArraySize )
            {
               // create big array from mall the small arrays
               var data = CreateData( currentSize, serializedEntries );

               // add the created result
               results.Add( data );

               // reset parameters
               currentSize = 0;
               serializedEntries = new List<byte[]>( serializedEntries.Count );
            }

            EntrySerializer.SerializeEntry( writer, entry, true, true );
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
            var data = CreateData( currentSize, serializedEntries );

            results.Add( data );
         }

         writer.Dispose();

         return results;
      }

      private static byte[] CreateData( int size, List<byte[]> serializedEntries )
      {
         var data = new byte[ size ];
         var copied = 0;
         for( int j = serializedEntries.Count - 1 ; j > -1 ; j-- )
         {
            var serializedEntry = serializedEntries[ j ];
            Buffer.BlockCopy( serializedEntry, 0, data, copied, serializedEntry.Length );
            copied += serializedEntry.Length;
         }
         return data;
      }

      public static List<IEntry> Deserialize( byte[] bytes )
      {
         var stream = new MemoryStream( bytes );
         var reader = EntrySerializer.CreateReader( stream );
         List<IEntry> entries = new List<IEntry>();

         while( stream.Length != stream.Position )
         {
            var entry = EntrySerializer.DeserializeEntry( reader, true );
            entries.Add( entry );
         }

         return entries;
      }
   }
}