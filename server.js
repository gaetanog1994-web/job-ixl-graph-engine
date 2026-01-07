import express from "express";
import neo4j from "neo4j-driver";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = Number(process.env.PORT ?? 8787);

const {
    GRAPH_SERVICE_TOKEN,
    NEO4J_URI,
    NEO4J_USER,
    NEO4J_PASSWORD,
    NEO4J_DATABASE,
} = process.env;

if (!GRAPH_SERVICE_TOKEN) throw new Error("Missing GRAPH_SERVICE_TOKEN");
if (!NEO4J_URI || !NEO4J_USER || !NEO4J_PASSWORD) throw new Error("Missing Neo4j env vars");

const driver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD));

async function ensureNeo4jReady(retries = 20) {
    try {
        await driver.verifyConnectivity();
        return;
    } catch {
        if (retries <= 0) throw new Error("Neo4j not ready");
        await new Promise((r) => setTimeout(r, 2000));
        return ensureNeo4jReady(retries - 1);
    }
}

function requireGraphToken(req, res, next) {
    const token = req.header("x-graph-token");
    if (!token || token !== GRAPH_SERVICE_TOKEN) {
        return res.status(401).json({ status: "ERROR", message: "Unauthorized" });
    }
    return next();
}

// Health resta pubblico (utile per Render)
app.get("/health", async (_req, res) => {
    try {
        await ensureNeo4jReady(0);
        res.json({ status: "OK", neo4j: "ready" });
    } catch {
        res.status(503).json({ status: "ERROR", neo4j: "sleeping" });
    }
});

// Tutto il resto è internal
app.use(requireGraphToken);

/** POST /neo4j/warmup */
app.post("/neo4j/warmup", async (_req, res) => {
    try {
        await ensureNeo4jReady(20);
        res.json({ status: "OK", neo4j: "ready" });
    } catch {
        res.json({ status: "WAIT", message: "Neo4j is waking up" });
    }
});

/**
 * POST /build-graph
 * body: { applications: [{user_id,target_user_id,priority}], usersById: { [id]: full_name } }
 * (dataset è deciso dal backend-api)
 */
app.post("/build-graph", async (req, res) => {
    await ensureNeo4jReady();
    const { applications = [], usersById = {} } = req.body || {};
    const session = driver.session({ database: NEO4J_DATABASE });

    try {
        const out = await session.executeWrite(async (tx) => {
            await tx.run("MATCH (n) DETACH DELETE n");

            await tx.run(
                `
        UNWIND $apps AS app
        MERGE (a:Person {id: app.user_id})
        SET a.full_name = coalesce($usersById[app.user_id], a.full_name)

        MERGE (b:Person {id: app.target_user_id})
        SET b.full_name = coalesce($usersById[app.target_user_id], b.full_name)

        MERGE (a)-[r:CANDIDATO_A]->(b)
        SET r.priority = app.priority
        `,
                { apps: applications, usersById }
            );

            const nodes = await tx.run("MATCH (n:Person) RETURN count(n) AS c");
            const rels = await tx.run("MATCH (:Person)-[r:CANDIDATO_A]->(:Person) RETURN count(r) AS c");

            return {
                nodes: nodes.records[0].get("c").toNumber(),
                relationships: rels.records[0].get("c").toNumber(),
            };
        });

        res.json({ status: "OK", ...out });
    } catch (err) {
        res.status(500).json({ status: "ERROR", message: err?.message || "Unknown error" });
    } finally {
        await session.close();
    }
});

/** POST /graph/chains */
app.post("/graph/chains", async (_req, res) => {
    await ensureNeo4jReady();
    const session = driver.session({ database: NEO4J_DATABASE });

    try {
        const cypher = `
      MATCH path = (p:Person)-[rels:CANDIDATO_A*2..10]->(p)
      WITH path, nodes(path) AS ns, rels
      WHERE size(ns[0..-1]) = size(apoc.coll.toSet(ns[0..-1]))
      WITH
        ns[0..-1] AS persons,
        rels,
        CASE
          WHEN ANY(r IN rels WHERE r.priority IS NULL)
          THEN null
          ELSE round(
            reduce(total = 0.0, r IN rels | total + toFloat(r.priority))
            / size(rels)
            * 100
          ) / 100
        END AS avgPriority
      RETURN
        [p IN persons | coalesce(p.full_name, p.id)] AS people,
        size(persons) AS length,
        avgPriority
    `;

        const result = await session.run(cypher);

        // dedupe by sorted set of people
        const seen = new Set();
        const chains = result.records
            .map((rec) => {
                const people = rec.get("people");
                const key = people.slice().sort().join("|");
                return {
                    key,
                    people,
                    length: rec.get("length").toNumber(),
                    avgPriority: rec.get("avgPriority"),
                };
            })
            .filter((c) => (seen.has(c.key) ? false : (seen.add(c.key), true)))
            .map(({ key, ...rest }) => rest);

        res.json({ status: "OK", chains });
    } catch (err) {
        res.status(500).json({ status: "ERROR", message: err?.message || "Unknown error" });
    } finally {
        await session.close();
    }
});

/** POST /graph/summary (relazioni) */
app.post("/graph/summary", async (_req, res) => {
    await ensureNeo4jReady();
    const session = driver.session({ database: NEO4J_DATABASE });

    try {
        const cypher = `
      MATCH (a:Person)-[r:CANDIDATO_A]->(b:Person)
      RETURN 
        coalesce(a.full_name, a.id) AS from_name,
        coalesce(b.full_name, b.id) AS to_name,
        r.priority AS priority
      ORDER BY from_name, to_name
    `;

        const result = await session.run(cypher);
        const relationships = result.records.map((rec) => ({
            from_name: rec.get("from_name"),
            to_name: rec.get("to_name"),
            priority: rec.get("priority"),
        }));

        res.json({ status: "OK", relationships });
    } catch (err) {
        res.status(500).json({ status: "ERROR", message: err?.message || "Unknown error" });
    } finally {
        await session.close();
    }
});

app.listen(PORT, "0.0.0.0", () => {
    console.log(`Graph Engine up on http://localhost:${PORT}`);
});
