using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TagCollection : IEnumerable<KeyValuePair<string, string>>
   {
      private string _key;
      private Dictionary<string, string> _tags;

      public TagCollection( Dictionary<string, string> tags )
      {
         _tags = tags;
         var sb = new StringBuilder( _tags.Sum( x => x.Key.Length + x.Value.Length + 1 + Environment.NewLine.Length ) );

         foreach( var tag in _tags )
         {
            sb.Append( tag.Key );
            sb.Append( '`' );
            sb.AppendLine( tag.Value );
         }

         _key = sb.ToString();
      }

      public TagCollection( IEnumerable<KeyValuePair<string, string>> tags )
         : this( tags.ToDictionary( x => x.Key, x => x.Value ) )
      {
      }

      public bool TryFindTagValue( string key, out string value )
      {
         return _tags.TryGetValue( key, out value );
      }

      public string FindTagValue( string key )
      {
         string value;
         _tags.TryGetValue( key, out value );
         return value;
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         var other = obj as TagCollection;
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

      public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
      {
         return _tags.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _tags.GetEnumerator();
      }
   }
}
