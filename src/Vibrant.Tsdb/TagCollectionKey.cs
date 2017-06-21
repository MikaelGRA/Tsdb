using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TagCollectionKey
   {
      private string _key;
      private StringBuilder _keyBuilder;

      public TagCollectionKey( int expectedCapacity )
      {
         _keyBuilder = new StringBuilder();
      }
      
      public void Add( string key, string value )
      {
         _keyBuilder.Append( key );
         _keyBuilder.Append( '´' );
         _keyBuilder.AppendLine( value );
      }

      public void Finish()
      {
         _key = _keyBuilder.ToString();
      }

      public int Size()
      {
         return _key.Length;
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         var other = obj as TagCollectionKey;
         if( other == null )
         {
            return false;
         }

         return _key == other._key;
      }

      // override object.GetHashCode
      public override int GetHashCode()
      {
         return _key.GetHashCode();
      }
   }
}
