using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class ConsistencyTests
    {
        private const ConsistencyLevel CL = ConsistencyLevel.LocalQuorum;
        private const string ConnectionString = "Contact Points=10.200.3.11,10.200.3.12";

        [Cassandra.Mapping.Attributes.Table("TestEntity1")]
        public class TestEntity1
        {
            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Mapping.Attributes.Column("partitionkey")]
            public string PartitionKey { get; set; }
            [Cassandra.Mapping.Attributes.ClusteringKey]
            [Cassandra.Mapping.Attributes.Column("clusteringkey")]
            public string ClusteringKey { get; set; }
            [Cassandra.Mapping.Attributes.Column("field")]
            public string Field { get; set; }
        }
        
        [Test]
        public void TestConsistency()
        {
            var session = Connect();
            var table = new Table<TestEntity1>(session);
            table.CreateIfNotExists();

            Loop(() =>
            {
                var pk = Guid.NewGuid().ToString();
                Console.WriteLine("PK = " + pk);

                // Insert
                var insertCql = "INSERT INTO TestEntity1 (partitionkey, clusteringkey, field) VALUES ('" + pk + "', 'ck', 'f');";
                Console.WriteLine(insertCql);
                var insertStatement = new SimpleStatement(insertCql).SetConsistencyLevel(CL);
                session.Execute(insertStatement);

                // Delete
                var deleteCql = "DELETE FROM TestEntity1 WHERE partitionkey = '" + pk + "';";
                Console.WriteLine(deleteCql);
                var deleteStatement = new SimpleStatement(deleteCql).SetConsistencyLevel(CL);
                session.Execute(deleteStatement);

                // Was it deleted?
                var selectCql = "SELECT * FROM TestEntity1 WHERE partitionkey = '" + pk + "';";
                Console.WriteLine(selectCql);
                var selectStatement = new SimpleStatement(selectCql).SetConsistencyLevel(CL);
                var rowSet = session.Execute(selectStatement).ToList();
                var count = rowSet.Count;
                Console.WriteLine("Results = { " + string.Join(", ", rowSet.Select(_ => _.GetValue<string>("partitionkey"))) + " }");
                Assert.AreEqual(0, count, string.Format("Expected 0 records but found {0}", count));
            });
        }

        private static void Loop(Action fn)
        {
            var failures = 0;
            foreach (var i in Enumerable.Range(0, 10))
            {
                try
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("Starting Run #" + i);
                    fn();
                    Console.WriteLine("Succeeded Run #" + i);
                }
                catch (AssertionException ex)
                {
                    Console.WriteLine("Failed Run #" + i + " " + ex.Message);
                    failures++;
                }
            }
            Assert.AreEqual(0, failures);
        }

        private static ISession Connect()
        {
            var anotherKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            var cluster = Cluster.Builder()
                                 .WithConnectionString(ConnectionString)
                                 .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                 .Build();
            var session = cluster.Connect();
            session.CreateKeyspace(anotherKeyspace, new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"},
                {"replication_factor", "2"}
            });
            session.ChangeKeyspace(anotherKeyspace);

            Console.WriteLine("Keyspace = " + anotherKeyspace);

            return session;
        }
    }
}