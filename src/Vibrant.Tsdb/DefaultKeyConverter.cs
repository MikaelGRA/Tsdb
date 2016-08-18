using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class DefaultKeyConverter<TKey> : IKeyConverter<TKey>
   {
      public static readonly DefaultKeyConverter<TKey> Current = new DefaultKeyConverter<TKey>();

      private Func<string, TKey> _toKey;
      private Func<TKey, string> _toString;

      private DefaultKeyConverter()
      {
         var type = typeof( TKey );

         if( type == typeof( sbyte ) )
         {
            _toString = key => ( (sbyte)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)sbyte.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( byte ) )
         {
            _toString = key => ( (byte)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)byte.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( short ) )
         {
            _toString = key => ( (short)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)short.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( ushort ) )
         {
            _toString = key => ( (ushort)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)ushort.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( int ) )
         {
            _toString = key => ( (int)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)int.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( uint ) )
         {
            _toString = key => ( (uint)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)uint.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( long ) )
         {
            _toString = key => ( (long)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)long.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( ulong ) )
         {
            _toString = key => ( (ulong)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)ulong.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( decimal ) )
         {
            _toString = key => ( (decimal)(object)key ).ToString( CultureInfo.InvariantCulture );
            _toKey = str => (TKey)(object)decimal.Parse( str, CultureInfo.InvariantCulture );
         }
         else if( type == typeof( char ) )
         {
            _toString = key => ( (char)(object)key ).ToString();
            _toKey = str => (TKey)(object)char.Parse( str );
         }
         else if( type == typeof( string ) )
         {
            _toString = key => (string)(object)key;
            _toKey = str => (TKey)(object)str;
         }
         else if( type == typeof( Guid ) )
         {
            _toString = key => ( (Guid)(object)key ).ToShortString();
            _toKey = str => (TKey)(object)GuidHelper.ParseShortGuid( str );
         }
         else
         {
            throw new ArgumentException( "The type argument TKey is not supported." );
         }
      }

      public string Convert( TKey key )
      {
         return _toString( key );
      }

      public TKey Convert( string key )
      {
         return _toKey( key );
      }
   }
}
