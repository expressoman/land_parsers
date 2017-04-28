import collectors.HttpInsecure as Http
import com.intuit.tools.CsvReader
import com.intuit.tools.Datavore
import groovy.json.* 

// def http = new Http("http://www.landsofamerica.com")
// 	def resp = http.get("/Washington/all-land/under-25000/5-20-acres/is-under-contract/is-sold/sort-price-low/page-1/")
// 	new File("landsofamerica.Washington.html").text = resp.body
// 	// println resp.body
// 	println new File(".").absolutePath

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

def backtax_list() {
	new File("back-tax-lists.html").text =  new Http("https://landacademy.com").get("/back-tax-lists/").body
	
	
	def doc = org.jsoup.Jsoup.parse(new File("back-tax-lists.html").text)
	def t = doc.select("table")
	def headers = []
	def f = new File("back-tax-lists.csv")
	f.text = ""
	def c = 0
	t.select("tr").each{
		if(it.text().contains("Control")) {
			headers = it.select("td").collect{it.text().replace(" #","")}
			headers[0] = "Control"
			f << headers.join(",") << "\n"
			c=1
		}
		if(headers.size() > 1 && c > 2) {
			def r = it.select("td").collect{it.text().replace(",", "-")}.join(",")
			if(!r.endsWith("n/a") && !r.endsWith("N/A")) {
				f << r << "\n"
			}
		}
		c+=1
	}
	def dv = CsvReader.parseString(f.text)
	def byState = dv.groupAndAggregate("State,County", "count,Parcels.Sum")
	println byState.sortDesc("Parcels.Sum")
	f.text = byState.sortDesc("Parcels.Sum")
}

def census_data() {
	def census = new Http("https://www.census.gov") 

	def states = census.get("/quickfacts/data/quickfacts/manifest/geography?fips=00")
	def states_and_counties = new File("states_and_counties.txt")
	states_and_counties.text = ""
	println states.json.manifest["00"].data.states.each {
		it.each {k,v->
			println "$k $v"
			def counties = census.get("/quickfacts/data/quickfacts/manifest/geography?fips=$k")
			println counties.body
			states_and_counties << "$k $v" << "\n" << counties.body << "\n"
			Thread.sleep(1000)
		}
		
	}
	
	def census_dv = new Datavore()
	def jsonSlurper = new groovy.json.JsonSlurper()
	
	states_and_counties = new File("states_and_counties.txt").text
	states_and_counties.split("\n").each {
		if(it.contains("manifest")) {
			jsonSlurper.parseText(it).manifest.each {_,c ->
				c.data.counties.each {cnty -> 
					cnty.each {k,v ->
						def facts = census.get("/quickfacts/data/quickfacts?fips=$k").json
						def x = facts[k].data
						// println "/quickfacts/data/quickfacts?fips=$k"
						def h = [state:c.postal, county:v.split(" County")[0], cid: k, pop:x.POP060210, land:x.LND110210]
						println h
						census_dv.add h
						// Thread.sleep(1000)
						// "${c.postal} ${v.split(" ")[0]} $k ${x.POP060210} ${x.LND110210}"
					}
				}
			}
		}
	}
	println census_dv
	new File("census_dv.csv").text = census_dv.asCsv()
}

def filtered(states) {
	def census_dv = CsvReader.parse("census_dv.csv")
// println census_dv
	
	census_dv_filtered = census_dv.in("state",*states)//.gte("pop", 1).lte("pop", 50).sortDesc("land")
	// println census_dv_filtered
	def res_dv = new Datavore()
	def tax_dv = CsvReader.parse("back-tax-lists.csv")
	// println tax_dv
	def byState = tax_dv.in("State",  *states).groupAndAggregate("State,County", "count,Parcels.Sum")
	census_dv_filtered.asMap().each { c ->
		def r = byState.eq("State", c.state).contains("County", c.county).asMap()[0]
		if(r) {
			res_dv.add( r + [land: c.land, pop: c.pop, cid: c.cid] )
		} else {
			// println c
		}
	}
	
	// println res_dv
	res_dv
}

def download_landwatch(filtered_dv) {
	def landwatch = new Http("http://www.landwatch.com")
	
	filtered_dv.asMap().each {m->
		def date = new Date().format("MM-dd-YYYY")
		def search = "${m.County.replace(" ", "+")}+County+${m.State}"
		def outfilename = new File("landwatch_${m.County}County${m.State}_${new Date().format("MM-dd-YYYY")}.json")
		if(!outfilename.exists()) {
			def results = []
			(1..20).find {cnt ->
				def x = landwatch.get("/default.aspx?ct=r&q=${search}&sort=PR_A&type=13,12&r.PSIZ=0.2%2c40&pn=1000&px=25000&pg=${cnt}").body
				def doc =  org.jsoup.Jsoup.parse(x)
				def r = doc.select("div.descwidth")
				println "${cnt} ${search} ${r.size()}"
				if(r.size()>0) {
					r.select("div.descwidth").each {it ->
						def propName = it.select("div.propName").text()
						def agent = it.select("div.propertyAgent > p > a").text()
						def description = it.select("div")[-7].text()
						def link = it.select("div.propName > a").first().attr("href")
						results << ([propName: propName, agent: agent, description: description, link: link, page: cnt, date: date] + m)
					}
					return false
				} else {
					return true
				}
				// println r
			}
			
			outfilename.text = new JsonBuilder( results ).toPrettyString()
			
			println outfilename.text
		}
	}
}

// def filtered_dv = filtered(["NV","TX","WA","FL","TN","NH","AZ"])
// download_landwatch(filtered_dv)
def landandfarm() {
	def landandfarm_state_map = [
		AZ:4,
		TX:56,
		WA:62,
		NV:38,
		NH:39,
		FL:12,
		TN:55
	]
	
	landandfarm_state_map.each {state, state_id ->
		def headers = [
			Accept:"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
			// "Accept-Encoding":"gzip, deflate, sdch",
			"Accept-Language":"en-US,en;q=0.8",
			"Cache-Control":"max-age=0",
			Connection:"keep-alive",
			"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
			]
			
			def results = []
			def f = new File("landandfarm_${state}_${new Date().format("MM-dd-YYYY")}.json")
			if(!f.exists()) {
				(1..2000).find{ page ->
					println "${state} ${page}"
					def landandfarm = new Http("http://www.landandfarm.com")
					def resp = landandfarm.get("/search/?StateIDs=${state_id}&MaxPrice=25000&Sale=True&Lease=False&Auction=False&CurrentPage=${page}", headers)
					
					def doc =  org.jsoup.Jsoup.parse(resp.body)
					
					def r = doc.select("article")
					if(r.size() > 0) {
						r.each {
					// println it
							def price  = it.select("span.price").first().text()
							def size = it.select("span.size").first().text()
							def agent = it.select("a.contact").first()
							results << [price: price, size: size, agent:agent?.attr("data-sellername"), address: agent?.attr("data-propertyaddress"), propertyname: agent?.attr("data-propertyname"), link: it.select("div.imageViewer > a")[0]?.attr("href"), description: it.select("address").select("meta")[1]?.attr("content") ]
						}
						false
					} else {
						true
					}
				}
				new File("landandfarm_${state}_${new Date().format("MM-dd-YYYY")}.json").text = new JsonBuilder( results ).toPrettyString()
		}
	}
}

def landandfarm_x() {


new File(".").listFiles().findAll{it.name.contains("landandfarm_") && it.name.contains("AZ")}.each {
	def json = new JsonSlurper().parseText(it.text)
	if(json.size() > 0) {
		json.each {
			println "${it.size} ${it.pricee} ${it.address}"
		}
	}
	
}
}

// landandfarm()
// def landwatch = new Http("http://www.landwatch.com")
// def resp = landwatch.get("/default.aspx?ct=r&q=Elko+County+NV&sort=PR_A&type=13,12&r.PSIZ=0.2%2c40&pn=1000&px=25000")
// new File("x.html").text = resp.body
// def x = new File("x.html").text
// def doc =  org.jsoup.Jsoup.parse(x)
// //println doc.select("div.resultssect") //.select("div.propertyAgent")//.select("div.resultssect")//.select("div.propertyAgent > p > a").collect{it.text()}
// doc.select("div.descwidth").each {it -> 
// 						def propName = it.select("div.propName").text()
// 						def agent = it.select("div.propertyAgent > p > a").text()
// 						def description = it.select("div")[-7].text()
// 						def link = it.select("div.propName > a").first().attr("href")
// 						println link
// }






// .each {
// 	def cls = it.attr("class")
// 	if(cls != "" && it.attr("class").contains("resultssect")) {
// 		println([it.attr("class"), it.attr("class").contains("resultssect")])
// 		println it
// 	}

def get_counties_from_landwatch() {
// }
def dvx = new Datavore()
new File(".").list().findAll{it.startsWith("landwatch_") && it.endsWith(".json")}.each {f ->
	// println f
	new JsonSlurper().parse(new File(f)).each {
		// println it
		if(it.propName?.contains(" Acre")) {
			it.acres = it.propName.split(" Acre")[0].toDouble()
		} else {
			it.acres = -1
		}
		if(it.propName?.contains("""\$""")) {
			try {
				
				def tmp = it.propName.replace("Negotiable","").split(" ").find{it.startsWith("""\$""")}
				it.price = tmp.replace("""\$""", "").replace(",","").toDouble() //it.desc.replace("Negotiable","").split(" ").find{it.startsWith("""\$""")}.replace("""\$""", "").replace(",","").toDouble()
				// println "${it.County} ${tmp} ${it.price}"
				it.price_per_acre = it.price/Math.max(it.acres, 1)
			} catch(e) {
				it.price = -1
				it.price_per_acre = -1
				// println e
				// println it.desc
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
			it.TaxCount = it."Parcels.Sum"
			dvx.add(it)
		}
		}
	}
	
	// println dvx

// println dvx.top("TaxCount", 50)
dvx = dvx.ne("State", "NV").notIn("County", "Coconino","Cochise","Culberson")
def agg = dvx.groupAndAggregate("State,County", "TaxCount.Min:asTaxCount,land.Min:aslandAreaSqMiles,pop.Avg:aspopDensity,count:aslwCount,price.Avg,price.Min,price.TP25,price.TP50,price.TP95,price_per_acre.Avg,price_per_acre.Min,price_per_acre.TP25,price_per_acre.TP50,price_per_acre.TP95,acres.Avg,acres.Min,acres.TP25,acres.TP95,acres.Max")
def filtered = agg.lte("popDensity",25).gte("lwCount", 10).gt("landAreaSqMiles", 800).gt("acres.Avg", 1.2) //.lt("count", 50)
new File("StateCountyStats.html").text = filtered.sortDesc("acres.Avg").asHtml()

// filtered.sortDesc("acres.Avg").asMap().each {m ->
// 	println dvx.eq("State",m.State).eq("County", m.County).lt("price_per_acre", m."price_per_acre.TP25").sortAsc("price_per_acre").select("State,County,acres,price,price_per_acre,desc,link").asHtml()
// }
// ,price_per_acre.Avg	,price_per_acre.Min,price_per_acre.Max").sortDesc("count")

println new File("StateCountyStats.html").text
}






