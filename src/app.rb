STDOUT.sync = true
require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'linkeddata', '~> 3.0'
end

class Syncer
  PREFIXES=%(
    PREFIX mu:      <http://mu.semte.ch/vocabularies/core/>
PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms:   <http://www.w3.org/ns/adms#>
PREFIX org: <http://www.w3.org/ns/org#>
)
  def initialize
    @gn = SPARQL::Client.new("http://db-gn:8890/sparql")
    @loket = SPARQL::Client.new("http://db-loket:8890/sparql")
  end

  def besturen
    @besturen ||= fetch_besturen
  end

  def query_gn(query_string)
    @gn.query("#{PREFIXES}\n#{query_string}")
  end

  def wait_for_dbs
    while !gn_up and !loket_up
      puts "waiting for endpoints..."
      sleep 2
    end
  end
  def gn_up
    @gn.ask.whether([:s,:p,:o]).true?
  rescue
    false
  end

  def loket_up
    @loket.ask.whether([:s,:p,:o]).true?
  rescue
    false
  end

  def query_loket(query_string)
    @loket.query("#{PREFIXES}\n#{query_string}")
  end

  def fetch_besturen
    query_loket(%(
           SELECT DISTINCT ?uri ?id ?naam WHERE {
             ?uri a besluit:Bestuurseenheid;
             mu:uuid ?id;
             besluit:classificatie ?classificatie.
            ?uri skos:prefLabel ?bestuurnaam.
            ?classificatie skos:prefLabel ?klassenaam.
            BIND(CONCAT(?klassenaam,"-",?bestuurnaam) as ?naam)
             FILTER(
               ?classificatie IN (
                              <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000001>, # gemeente
                              <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000002> # ocmw
                              )
             )
           }
   ))
  end

  def fetch_organen(eenheid)
    query_loket(%(
    SELECT DISTINCT ?uri ?id WHERE {
            ?uri a besluit:Bestuursorgaan;
                 besluit:bestuurt <#{eenheid}>;
                 mu:uuid ?id.
    }
    ))
  end

  def fetch_organen_in_tijd(orgaan)
    query_loket(%(
    SELECT DISTINCT ?uri ?id WHERE {
            ?uri a besluit:Bestuursorgaan;
                 mandaat:isTijdspecialisatieVan <#{orgaan}>;
                 mu:uuid ?id.
    }
    ))
  end

  def fetch_mandaten(orgaan)
    query_loket(%(
    SELECT DISTINCT ?uri ?id WHERE {
            <#{orgaan}> a besluit:Bestuursorgaan;
                 org:hasPost ?uri.
            ?uri mu:uuid ?id.
    }
    ))
  end

  def fetch_mandatarissen(mandaat)
    query_loket(%(
    SELECT DISTINCT ?uri ?id ?persoon WHERE {
            ?uri a mandaat:Mandataris.
            ?uri org:holds <#{mandaat}>.
            ?uri mu:uuid ?id;
                 mandaat:isBestuurlijkeAliasVan ?persoon.
    }
    ))
  end

  def fetch_persoon(persoon)
    query_loket(%(
      SELECT DISTINCT ?uri ?id ?identificator ?geboorte WHERE {
        BIND(<#{persoon}> as ?uri)
        ?uri a person:Person;
             mu:uuid ?id.
        OPTIONAL {
           ?uri adms:identifier ?identificator.
        }
        OPTIONAL {
           ?uri <http://data.vlaanderen.be/ns/persoon#heeftGeboorte> ?geboorte.
        }
      }))
  end

  def fetch_full_resource(resource, public = true)
    query_loket(%(
      CONSTRUCT {
     GRAPH #{ public ? "<http://mu.semte.ch/graphs/public>" : "?g"} {
          ?s ?p ?o.
          ?foo ?bar ?s.
        }
      }
      WHERE {
        BIND(<#{resource}> as ?s)
        GRAPH #{ public ? "<http://mu.semte.ch/graphs/public>" : "?g"} {
          ?s ?p ?o.
          OPTIONAL {
            ?foo ?bar ?s.
          }
        }
      }
    ))
  end

  def resource_exists_in_gn(type, uri)
    query_gn("ASK {<#{uri}> a #{type}}")
  end

  def orgaan_exists_in_gn(uri)
    resource_exists_in_gn("besluit:Bestuursorgaan", uri)
  end

  def mandaat_exists_in_gn(uri)
    resource_exists_in_gn("mandaat:Mandaat", uri)
  end

  def mandataris_exists_in_gn(uri)
    resource_exists_in_gn("mandaat:Mandataris", uri)
  end

  def persoon_exists_in_gn(uri, bestuursid)
    query_gn(%(ASK { GRAPH <#{construct_gn_graph(bestuursid)}> { <#{uri}> a person:Person } }))
  end

  def construct_gn_graph(id)
    "http://mu.semte.ch/graphs/organizations/#{id}"
  end

  def sync
    puts "found #{besturen.size} besturen to sync"
    besturen.each do |bestuur|
      public_graph = RDF::Repository.new()
      private_graph = RDF::Repository.new()
      puts "checking #{bestuur[:naam]} #{bestuur[:id]}"
      fetch_organen(bestuur[:uri]).each do |orgaan|
        if !orgaan_exists_in_gn(orgaan[:uri])
          triples = fetch_full_resource(orgaan[:uri])
          public_graph << triples
        end
        fetch_organen_in_tijd(orgaan[:uri]).each do |orgaan_in_tijd|
          if !orgaan_exists_in_gn(orgaan[:uri])
            puts "orgaan in tijd #{orgaan[:uri]} missing in GN"
            triples = fetch_full_resource(orgaan[:uri])
            public_graph << triples
          end
          fetch_mandaten(orgaan_in_tijd[:uri]).each do |mandaat|
            if !mandaat_exists_in_gn(mandaat[:uri])
              puts "mandaat #{mandaat[:uri]} missing in GN"
              triples = fetch_full_resource(mandaat[:uri])
              public_graph << triples
            end
            fetch_mandatarissen(mandaat[:uri]).each do |mandataris|
              if !mandataris_exists_in_gn(mandataris[:uri])
                puts "mandataris #{mandataris[:uri]} missing in GN"
                triples = fetch_full_resource(mandataris[:uri], false)
                public_graph << triples
              end
              if !persoon_exists_in_gn(mandataris[:persoon], bestuur[:id])
                personen = fetch_persoon(mandataris[:persoon])
                if (!personen or personen.size != 1)
                  puts "#{personen.nil? ? personen.size : "no"} persons linked to #{mandataris[:uri]}"
                else
                  persoon = personen.first
                  puts "persoon #{persoon[:uri]} missing in GN"
                  triples = fetch_full_resource(persoon[:uri], false)
                  private_graph << triples
                  if persoon[:identificator]
                    triples = fetch_full_resource(persoon[:identificator], false)
                    if triples.size > 0
                      private_graph << triples
                    else
                      puts "no identificator found for #{persoon[:uri]}"
                    end
                  end
                  if persoon[:geboorte]
                    triples = fetch_full_resource(persoon[:geboorte], false)
                    if triples.size > 0
                      private_graph << triples
                    else
                      puts "no geboorte found for #{persoon[:uri]}"
                    end
                  end
                end
              end
            end
          end
        end
      end
      puts "writing data for #{bestuur[:naam]} #{bestuur[:id]}} (public #{public_graph.size}, private #{private_graph.size})"
      if public_graph.size > 0
        File.open('/output/20191024223800-public-mandaten-sync.ttl', "a") do |file|
          file.write "# mandaten voor #{bestuur[:naam]} #{bestuur[:id]}\n"
          file.write public_graph.dump(:ntriples)
        end
      end
      if private_graph.size > 0
        File.open("/output/20191024223800-private-mandaten-sync-#{bestuur[:naam]}-#{bestuur[:id]}.ttl", "a") do |file|
          file.write private_graph.dump(:ntriples)
        end
        File.open("/output/20191024223800-private-mandaten-sync-#{bestuur[:naam]}-#{bestuur[:id]}.graph", "a") do |file|
          file.write construct_gn_graph(bestuur[:id])
        end
      end
    end
    File.open('/output/20191024223800-public-mandaten-sync.graph', "a") do |file|
      file.write "http://mu.semte.ch/graphs/public"
    end
  end
end

syncer = Syncer.new
syncer.wait_for_dbs
syncer.sync
