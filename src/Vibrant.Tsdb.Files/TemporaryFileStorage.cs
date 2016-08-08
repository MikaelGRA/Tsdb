using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Exceptions;

namespace Vibrant.Tsdb.Files
{
   public class TemporaryFileStorage<TEntry> : ITemporaryStorage<TEntry>
      where TEntry : IFileEntry, new()
   {
      private static readonly string FileTemplate = "{0}.dat";

      private object _sync = new object();
      private DirectoryInfo _directory;
      private FileInfo _currentFile;
      private int _maxFileSize;
      private long _maxStorageSize;
      private long _currentSize;

      // https://msdn.microsoft.com/en-us/library/system.io.filestream.lock(v=vs.110).aspx
      public TemporaryFileStorage( string directory, int maxFileSize, long maxStorageSize )
      {
         _directory = new DirectoryInfo( directory );
         _maxFileSize = maxFileSize;
         _maxStorageSize = maxStorageSize;

         CalculateDirectorySize();
      }

      public TemporaryReadResult<TEntry> Read( int count )
      {
         lock( _sync )
         {
            int read = 0;
            List<TEntry> entries = new List<TEntry>();
            List<FileModificationRef> modifications = new List<FileModificationRef>();
            foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
            {
               using( var fs = fi.Open( FileMode.Open, FileAccess.Read ) )
               using( var reader = new BinaryReader( fs, Encoding.ASCII, true ) )
               {

                  while( reader.PeekChar() != -1 && read < count )
                  {
                     var entry = new TEntry();
                     var id = reader.ReadString();
                     var timestamp = new DateTime( reader.ReadInt64(), DateTimeKind.Utc );
                     entry.SetId( id );
                     entry.SetTimestamp( timestamp );
                     entry.Read( reader );
                     entries.Add( entry );
                     read++;
                  }

                  // add the file
                  var reference = new FileModificationRef( fi, fs.Length, fs.Position );
                  modifications.Add( reference );

                  if( read == count )
                  {
                     break;
                  }
               }
            }

            return new TemporaryReadResult<TEntry>( entries, () => Delete( modifications ) );
         }
      }

      public void Write( IEnumerable<TEntry> entries )
      {
         lock( _sync )
         {
            if( _currentSize > _maxStorageSize )
            {
               throw new TsdbException( "Could not write any of the entries to the temporary file storage because it exceeds the maximum storage size." );
            }

            // Open the current or a new file
            FileInfo fileInfo = GetOrCreateCurrentFile();
            FileStream fileStream = fileInfo.Open( FileMode.OpenOrCreate, FileAccess.ReadWrite );
            long startFileSize = fileStream.Length;

            // Seek to the end of the file
            fileStream.Seek( 0, SeekOrigin.End );

            // Create the writer
            BinaryWriter writer = new BinaryWriter( fileStream, Encoding.ASCII, true );

            bool disposed = false;

            long beforeLength = startFileSize;
            long afterLength = beforeLength;
            foreach( var entry in entries )
            {
               beforeLength = afterLength;

               if( beforeLength > _maxFileSize )
               {
                  // flush to file
                  writer.Flush();

                  // increment storage size
                  _currentSize += ( beforeLength - startFileSize );

                  // close the current file stream
                  writer.Dispose();
                  fileStream.Dispose();

                  // create a new file reference...
                  fileInfo = CreateCurrentFile();
                  fileStream = fileInfo.Open( FileMode.OpenOrCreate, FileAccess.ReadWrite );
                  writer = new BinaryWriter( fileStream, Encoding.ASCII, true );
                  startFileSize = fileStream.Length;
                  beforeLength = startFileSize;
                  afterLength = beforeLength;
               }

               long storageSize = _currentSize + ( beforeLength - startFileSize );
               if( storageSize > _maxStorageSize )
               {
                  // flush to file
                  writer.Flush();

                  // increment storage size
                  _currentSize += ( beforeLength - startFileSize );

                  // close the current file stream
                  writer.Dispose();
                  fileStream.Dispose();
                  disposed = true;

                  // throw exception indicating which entries could not be inserted (with skip)
                  throw new TsdbException( "Could not write all the entries to the temporary file storage because it exceeds the maximum storage size." );
               }

               writer.Write( entry.GetId() );
               writer.Write( entry.GetTimestamp().Ticks );
               entry.Write( writer );

               afterLength = fileStream.Length;
            }

            writer.Flush();
            writer.Dispose();
            fileStream.Dispose();
         }
      }

      public void Delete()
      {
         lock( _sync )
         {
            foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
            {
               _currentSize -= fi.Length;
               fi.Delete();
            }
         }
      }

      private void Delete( List<FileModificationRef> modifications )
      {
         lock( _sync )
         {
            foreach( var mod in modifications )
            {
               _currentSize -= mod.DeleteLength;
               mod.PerformModification();
            }
         }
      }

      private void CalculateDirectorySize()
      {
         if( !_directory.Exists )
         {
            _directory.Create();
         }

         _currentSize = 0;
         foreach( var fi in _directory.EnumerateFiles( "*.dat" ) )
         {
            _currentSize += fi.Length;
         }
      }

      private FileInfo GetOrCreateCurrentFile()
      {
         if( _currentFile != null )
         {
            if( !_currentFile.Exists )
            {
               _currentFile = CreateCurrentFile();
            }

            return _currentFile;
         }

         return CreateCurrentFile();
      }

      private FileInfo CreateCurrentFile()
      {
         var timestamp = DateTime.UtcNow;
         var fi = new FileInfo( Path.Combine( _directory.FullName, CreateFileName( timestamp ) ) );
         while( fi.Exists )
         {
            timestamp += TimeSpan.FromMilliseconds( 10 );
            fi = new FileInfo( Path.Combine( _directory.FullName, CreateFileName( timestamp ) ) );
         }

         _currentFile = fi;

         return fi;
      }

      private string CreateFileName( DateTime timestamp )
      {
         return string.Format( FileTemplate, timestamp.ToString( "yyyyMMddHHmmssffff" ) );
      }
   }
}
