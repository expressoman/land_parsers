import collectors.HttpInsecure as Http
import com.intuit.tools.CsvReader
import com.intuit.tools.Datavore
import groovy.json.* 
// def http = new Http("http://www.landsofamerica.com")
// def resp = http.get("/Washington/all-land/under-25000/5-20-acres/is-under-contract/is-sold/sort-price-low/page-1/")
// new File("landsofamerica.Washington.html").text = resp.body
// // println resp.body
// println new File(".").absolutePath

// new File("back-tax-lists.html").text =  new Http("https://landacademy.com").get("/back-tax-lists/").body


// def doc = org.jsoup.Jsoup.parse(new File("back-tax-lists.html").text)
// def t = doc.select("table")
// def headers = []
// def f = new File("back-tax-lists.csv")
// f.text = ""
// def c = 0
// t.select("tr").each{
// 	if(it.text().contains("Control")) {
// 		headers = it.select("td").collect{it.text().replace(" #","")}
// 		headers[0] = "Control"
// 		f << headers.join(",") << "\n"
// 		c=1
// 	}
// 	if(headers.size() > 1 && c > 2) {
// 		def r = it.select("td").collect{it.text().replace(",", "-")}.join(",")
// 		if(!r.endsWith("n/a") && !r.endsWith("N/A")) {
// 			f << r << "\n"
// 		}
// 	}
// 	c+=1
// }

def census = new Http("https://www.census.gov") 

// def states = census.get("/quickfacts/data/quickfacts/manifest/geography?fips=00")
// def states_and_counties = new File("states_and_counties.txt")
// states_and_counties.text = ""
// println states.json.manifest["00"].data.states.each {
// 	it.each {k,v->
// 		println "$k $v"
// 		def counties = census.get("/quickfacts/data/quickfacts/manifest/geography?fips=$k")
// 		println counties.body
// 		states_and_counties << "$k $v" << "\n" << counties.body << "\n"
// 		Thread.sleep(1000)
// 	}
	
// }

// def facts = census.get("/quickfacts/data/quickfacts?0101").json

// facts."00".data.each{
// println it
// }

// def census_dv = new Datavore()
// def jsonSlurper = new groovy.json.JsonSlurper()

// def states_and_counties = new File("states_and_counties.txt").text
// states_and_counties.split("\n").each {
// 	if(it.contains("manifest")) {
// 		jsonSlurper.parseText(it).manifest.each {_,c ->
// 			c.data.counties.each {cnty -> 
// 				cnty.each {k,v ->
// 					def facts = census.get("/quickfacts/data/quickfacts?fips=$k").json
// 					def x = facts[k].data
// 					// println "/quickfacts/data/quickfacts?fips=$k"
// 					def h = [state:c.postal, county:v.split(" County")[0], cid: k, pop:x.POP060210, land:x.LND110210]
// 					println h
// 					census_dv.add h
// 					// Thread.sleep(1000)
// 					// "${c.postal} ${v.split(" ")[0]} $k ${x.POP060210} ${x.LND110210}"
// 				}
// 			}
// 		}
// 	}
// }
// println census_dv
// new File("census_dv_v2.csv").text = census_dv.asCsv()

// def f = new File("back-tax-lists.csv")
// def dv = CsvReader.parseString(f.text)
// def byState = dv.groupAndAggregate("State,County", "count,Parcels.Sum")
// println byState.sortDesc("Parcels.Sum")


def census_dv = CsvReader.parse("census_dv_v2.csv")

census_dv_filtered = census_dv.in("state", "FL", "NV", "TX", "WA", "NH", "TN").gte("pop", 1).lte("pop", 50).sortDesc("land")
def res_dv = new Datavore()
def tax_dv = CsvReader.parse("back-tax-lists.csv")
def byState = tax_dv.in("State",  "FL", "NV", "TX", "WA", "NH", "TN").groupAndAggregate("State,County", "count,Parcels.Sum")
census_dv_filtered.asMap().each { c ->
	def r = byState.eq("State", c.state).contains("County", c.county).asMap()[0]
	if(r) {
		res_dv.add( r + [land: c.land, pop: c.pop, cid: c.cid] )
	} else {
		// println c
	}
}

println res_dv.top("Parcels.Sum", 10)


// println byState.eq("State", "WA")
// println census_dv.lte("pop", 200)
// println dv.eq("State","TN").sortDesc("Parcels")
// dv = dv.gt("Parcels", 1000)
// dv.distinct("County").each {
// 	println it
// 	println dv.eq("County", it).sortDesc("Parcels")
// }
// println dv.groupBy("County").groups.each {
// 	println it.vals
// }
// println dv
// println dv.groupBy("Type").groups.each{
// 	if(it.vals.get(0) =="Online") {
// 		println it.dv
// 	}
// }
	//.eq("State","AZ").eq("Type","OTC").sortDesc("Parcels")

// println f.text
// println dv

def landwatch = new Http("http://www.landwatch.com")
// def resp = landwatch.get("/default.aspx?ct=r&q=Elko+County+NV&sort=PR_A&type=13,12&r.PSIZ=0.2%2c40&pn=1000&px=25000")

// println resp.body

// def doc =  org.jsoup.Jsoup.parse(new File("landwatch.html").text)
// println doc.select("div.resultssect").first().select("div.propName").select("a").each {
// 	println it.attr("href")
// }

/*

res_dv.top("Parcels.Sum", 50).asMap().each {m->
def search = "${m.County.replace(" ", "+")}+County+${m.State}"
def outfilename = "landwatch_${m.County}County${m.State}.json"

def results = []
(1..20).find {cnt ->
	def x = landwatch.get("/default.aspx?ct=r&q=${search}&sort=PR_A&type=13,12&r.PSIZ=0.2%2c40&pn=1000&px=25000&pg=${cnt}").body
	def doc =  org.jsoup.Jsoup.parse(x)
	def r = doc.select("div.resultssect")
	println "${cnt} ${search} ${r.size()}"
	if(r.size()>0) {
		r.first().select("div.propName").select("a").each {
			results << ([desc: it.text(), link: it.attr("href"), page: cnt] + m)
		}
		return false
	} else {
		return true
	}
	// println r
}

new File(outfilename).text = new JsonBuilder( results ).toPrettyString()

println new File(outfilename).text

}
*/



// .each {
// 	def cls = it.attr("class")
// 	if(cls != "" && it.attr("class").contains("resultssect")) {
// 		println([it.attr("class"), it.attr("class").contains("resultssect")])
// 		println it
// 	}
// }
def dvx = new Datavore()
new File(".").list().findAll{it.endsWith(".json")}.each {f ->
	new JsonSlurper().parse(new File(f)).each {
		if(it.desc.contains(" Acre")) {
			it.acres = it.desc.split(" Acre")[0].toDouble()
		} else {
			it.acres = -1
		}
		if(it.desc.contains("""\$""")) {
			try {
				
				def tmp = it.desc.replace("Negotiable","").split(" ").find{it.startsWith("""\$""")}
				it.price = tmp.replace("""\$""", "").replace(",","").toDouble() //it.desc.replace("Negotiable","").split(" ").find{it.startsWith("""\$""")}.replace("""\$""", "").replace(",","").toDouble()
				// println "${it.County} ${tmp} ${it.price}"
				it.price_per_acre = it.price/Math.max(it.acres, 1)
			} catch(e) {
				it.price = -1
				it.price_per_acre = -1
				println e
				println it.desc
			}
		} else {
				it.price = -1
				it.price_per_acre = -1
		}
		if(it.County && it.price > 0 && it.acres > 0 && it.price_per_acre > 0) {
		// if(it.County == "Leon"){
		// 	println "${it.County} ${it.price} ${dvx.count()}"
		// 	println it
		// 	it.desc = ""
		// 	// println dvx.eq("County", "Leon").select("price")
		// }
			dvx.add(it)
		}
		// }
	}
	
}

def agg = dvx.groupAndAggregate("State,County", "land.Min:asland,pop.Avg:aspop,count,price.Avg,price.Min,price.TP25,price.TP50,price.TP95,price_per_acre.Avg,price_per_acre.Min,price_per_acre.TP25,price_per_acre.TP50,price_per_acre.TP95,acres.Avg,acres.Min,acres.TP25,acres.TP95,acres.Max")
def filtered = agg.gte("count", 10).gt("land", 800).gt("acres.Avg", 1.2) //.lt("count", 50)
new File("StateCountyStates.html").text = filtered.sortDesc("acres.Avg").asHtml()

filtered.sortDesc("acres.Avg").asMap().each {m ->
	println dvx.eq("State",m.State).eq("County", m.County).lt("price_per_acre", m."price_per_acre.TP25").sortAsc("price_per_acre").select("State,County,acres,price,price_per_acre,desc,link").asHtml()
}
//,price_per_acre.Avg	,price_per_acre.Min,price_per_acre.Max").sortDesc("count")



