package kafkastreams.exercises

import com.fasterxml.jackson.databind.ObjectMapper

object ClickEvents {

  private val mapper = new ObjectMapper()

  val clickEvents = List(
    """{
      |  "@id": "509a1152-1ffb-4b11-be79-acf9753da1a9",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:1234567"
      |  },
      |  "object": {
      |    "@type": "ClassifiedAd",
      |    "name": "Klassisk DBS selges",
      |    "price": 1500,
      |    "url": "https://www.finn.no/bap/forsale/ad.html?finnkode=109312065"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:finn"
      |  },
      |  "published": "2017-11-26T09:00:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "2749453e-8e68-41db-9918-aff19f0c68e2",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:1234567"
      |  },
      |  "object": {
      |    "@type": "Article",
      |    "name": "Trump vil føre opp Nord-Korea på terrorlisten",
      |    "url": "https://www.aftenposten.no/verden/i/0EjoyJ/Trump-vil-fore-opp-Nord-Korea-pa-terrorlisten"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:aftenposten"
      |  },
      |  "published": "2017-11-26T09:10:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "a03ae09c-a594-470e-951d-8679990f1057",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:6543"
      |  },
      |  "object": {
      |    "@type": "ClassifiedAd",
      |    "name": "Bianchi aria",
      |    "price": 23000,
      |    "url": "https://www.blocket.se/stockholm/Bianchi_aria_76294293.htm?ca=11&w=1"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:blocket"
      |  },
      |  "published": "2017-11-26T09:15:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "a8196621-8587-453a-881d-0e6917d3fc0c",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:26347"
      |  },
      |  "object": {
      |    "@type": "ClassifiedAd",
      |    "name": "Divers jeux d'éveil et hochets",
      |    "price": 1,
      |    "url": "https://www.leboncoin.fr/jeux_jouets/1310126037.htm?ca=7_s"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:leboncoin"
      |  },
      |  "published": "2017-11-26T09:20:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "908bc929-5f79-4520-b916-ca64e61a8624",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:6543"
      |  },
      |  "object": {
      |    "@type": "Article",
      |    "name": "Vulkanen ryker – flyg ställs in",
      |    "url": "https://www.aftonbladet.se/nyheter/a/J1WkPX/vulkanen-ryker--flyg-stalls-in"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:aftonbladet"
      |  },
      |  "published": "2017-11-26T09:26:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "1ddfbaca-ac04-4d76-a1e6-f059ef772caa",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:131135"
      |  },
      |  "object": {
      |    "@type": "ClassifiedAd",
      |    "price": 198,
      |    "name": "Tica Discovery Fly. Stang for fluefiske for et par hundrelapper pga vannskade",
      |    "url": "https://www.finn.no/bap/webstore/ad.html?finnkode=107329093"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:finn"
      |  },
      |  "published": "2017-11-26T09:26:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin,

    """{
      |  "@id": "8e904a9a-7991-4603-8260-648669ce867f",
      |  "@type": "View",
      |  "actor": {
      |    "@id": "sdrn:schibsted:user:131135"
      |  },
      |  "object": {
      |    "@type": "ClassifiedAd",
      |    "price": 500,
      |    "name": "Vadebukse",
      |    "url": "https://www.finn.no/bap/forsale/ad.html?finnkode=105004553"
      |  },
      |  "provider": {
      |    "@id": "sdrn:schibsted:client:finn"
      |  },
      |  "published": "2017-11-26T10:19:00+00:00",
      |  "schema": "http://schema.schibsted.com/events/tracker-event.json/118.json#"
      |}""".stripMargin
  )
    .map(mapper.readTree)
    .map(json => (json.get("@id").asText, json))
}
