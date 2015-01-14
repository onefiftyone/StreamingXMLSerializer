using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Xml;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingDataTable : IDisposable, IEnumerable<DataRow>, IEnumerable<ExpandoObject>
    {
        private DataTable schemaTable;

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

        internal StreamingDataTable(string baseURI, DataTable t)
        {
            BaseURI = baseURI;
            schemaTable = t;
        }

        private DataRow getNextDataRow(XmlReader reader)
        {
            DataRow dr = schemaTable.NewRow();
            if (reader.ReadToFollowing(TableName))
            {
                using (var subReader = reader.ReadSubtree())
                {
                    subReader.Read(); //eat parent node
                    while (subReader.Read())
                    {
                        if (subReader.NodeType == XmlNodeType.Element)
                        {
                            dr[subReader.LocalName]
                                = subReader.ReadElementContentAs(schemaTable.Columns[subReader.LocalName].DataType, null);
                        }
                    }
                    subReader.Close();
                }
            }
            else
                return null;

            return dr;
        }

        private ExpandoObject getNextExpando(XmlReader reader)
        {
            var obj = new ExpandoObject() as IDictionary<string, object>;

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

            return (ExpandoObject) obj;
        }

        public IEnumerator<ExpandoObject> AsExpandoObjects()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    ExpandoObject obj = getNextExpando(reader);
                    if (obj != null)
                        yield return obj;
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            schemaTable.Dispose();
        }

        #endregion

        #region IEnumerable<DataRow> Members

        public IEnumerator<DataRow> GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var dr = getNextDataRow(reader);
                    if (dr != null)
                        yield return dr; 
                }
            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var dr = getNextDataRow(reader);
                    if (dr != null)
                        yield return dr;
                }
            }
        }

        #endregion

        #region IEnumerable<ExpandoObject> Members

        IEnumerator<ExpandoObject> IEnumerable<ExpandoObject>.GetEnumerator()
        {
            return this.AsExpandoObjects();
        }

        #endregion
    }
}
