using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingDataTable : IDisposable, IEnumerable<StreamingDataRow>
    {
        private DataTable schemaTable;
        internal int? count = null;

        #region CONSTRUCTORS

        internal StreamingDataTable(string baseURI, DataTable t)
        {
            if (string.IsNullOrWhiteSpace(baseURI))
                throw new Exception("Missing filename");
            if (!File.Exists(baseURI))
                throw new FileNotFoundException("File does not exist: " + baseURI);

            BaseURI = baseURI;
            schemaTable = t;
        }

        #endregion

        #region PROPERTIES

        public string BaseURI { get; private set; }

        public string TableName
        {
            get
            {
                return schemaTable.TableName;
            }
            set
            {
                if (value == string.Empty)
                    throw new Exception("Cannot set StreamingDataTable's name to be empty");
                schemaTable.TableName = value;
            }
        }

        public DataColumnCollection Columns
        {
            get
            {
                return schemaTable.Columns;
            }
        }

        public int Count
        {
            get
            {
                if (count == null)
                {
                    count = 0;
                    using (XmlReader reader = XmlReader.Create(BaseURI))
                    {
                        while (reader.ReadToFollowing(TableName))
                            count++;
                    }
                }

                return count.Value;
            }
        }

        #endregion

        private Dictionary<string, object> getNextInternal(XmlReader reader)
        {
            var obj = new Dictionary<string, object>(StringComparer.CurrentCultureIgnoreCase);

            if (reader.ReadToFollowing(TableName))
            {
                using (var subReader = reader.ReadSubtree())
                {
                    subReader.Read(); //eat parent node
                    while (subReader.Read())
                    {
                        if (subReader.NodeType == XmlNodeType.Element)
                        {
                            if (obj.ContainsKey(subReader.LocalName))
                                throw new DuplicateNameException("Key '" + subReader.LocalName + "; already exists");
                            obj[subReader.LocalName]
                                = subReader.ReadElementContentAs(schemaTable.Columns[subReader.LocalName].DataType, null);
                        }
                    }
                    subReader.Close();
                }
            }
            else
                return null;

            //normalize columns
            foreach (DataColumn c in schemaTable.Columns)
            {
                if (!obj.ContainsKey(c.ColumnName))
                    obj.Add(c.ColumnName, null);
            }

            return obj;
        }

        #region IDisposable Members

        public void Dispose()
        {
            schemaTable.Dispose();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var obj = getNextInternal(reader);
                    if (obj != null)
                        yield return new StreamingDataRow(schemaTable.Columns, obj);
                }
            }
        }

        IEnumerator<StreamingDataRow> IEnumerable<StreamingDataRow>.GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var obj = getNextInternal(reader);
                    if (obj != null)
                        yield return new StreamingDataRow(schemaTable.Columns, obj);
                }
            }
        }

        #endregion
    }
}
