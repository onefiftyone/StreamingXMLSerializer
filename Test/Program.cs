using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using OneFiftyOne.Serialization.StreamingXMLSerializer;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            string filename = "../../test-file.xml";
            using (var ds = new StreamingDataSet())
            {
                ds.ReadXML(filename);

                int c = 0;
                int t = ds["ROADWAY"].Count;

                using (var dsout = new StreamingDataSet())
                {
                    foreach (var table in ds.Tables)
                    {
                        var dt = new StreamingDataTable(table.TableName);
                        dt.DataSource = ds[table.TableName].Take(2);
                        dsout.AddTable(dt);
                    }

                    dsout.WriteXML("../../test-file-output.xml");
                }
            }


        }
    }
}
