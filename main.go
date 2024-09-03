package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	"golang.org/x/time/rate"
)

var (
	// we use maps to translate the months and days from spanish to english
	// a map created with map[key]value
	spanishMonths = map[string]string{
		"Enero":      "January",
		"Febrero":    "February",
		"Marzo":      "March",
		"Abril":      "April",
		"Mayo":       "May",
		"Junio":      "June",
		"Julio":      "July",
		"Agosto":     "August",
		"Septiembre": "September",
		"Octubre":    "October",
		"Noviembre":  "November",
		"Diciembre":  "December",
	}

	spanishDays = map[string]string{
		"Lunes":     "Monday",
		"Martes":    "Tuesday",
		"Miercoles": "Wednesday",
		"Jueves":    "Thursday",
		"Viernes":   "Friday",
		"Sabado":    "Saturday",
		"Domingo":   "Sunday",
	}
)

// a struct is a collection of fields
// its equivalent in TypeScript would be an interface
// the json tag is used to tell the json encoder and decoder the name of the field in the json
type Article struct {
	Title      string `json:"title"`
	Date       string `json:"date"`
	Category   string `json:"category"`
	CategoryId int    `json:"category_id"`
	Body       string `json:"body"`
	Link       string `json:"link"`
}

func Geocode(address string) (float64, float64) {
	address = url.QueryEscape(address)
	url := fmt.Sprintf("http://localhost:8080/search.php?q=%s", address)

	resp, err := http.Get(url)
	if err != nil {
		return 0, 0
	}
	defer resp.Body.Close()

	var results []map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return 0, 0
	}

	if len(results) > 0 {
		firstResult := results[0]
		displayName, ok := firstResult["display_name"].(string)
		if ok && strings.Contains(displayName, "6740") {
			lat, _ := strconv.ParseFloat(firstResult["lat"].(string), 64)
			lon, _ := strconv.ParseFloat(firstResult["lon"].(string), 64)
			return lat, lon
		}
	}

	return 0, 0
}

// you declare a function with func keyword, the name of the function,
// the type of the parameters and the type of the return value, you
// can return multiple values.
// the time.Time type is a struct that contains the date and time
// the error type is a built-in type that represents an error
func parseSpanishDate(dateStr string) (time.Time, error) {
	// range loop (for in, in javascript), it iterates over the map spanishMonths
	// for each key-value pair, it replaces the key with the value in the dateStr
	for spanish, english := range spanishMonths {
		// strings.ReplaceAll replaces all occurrences of the spanish month with the english month
		// its js equivalent is dateStr = dateStr.replace(/spanish/g, english);
		dateStr = strings.ReplaceAll(dateStr, spanish, english)
	}
	for spanish, english := range spanishDays {
		dateStr = strings.ReplaceAll(dateStr, spanish, english)
	}
	// strings.NewReplacer replaces multiple values at once
	// its js equivalent is dateStr = dateStr.replace(/de|./g, "");
	dateStr = strings.NewReplacer("de", "", ".", "").Replace(dateStr)
	return time.Parse("Monday, 02 January 2006 15:04 Hs", strings.TrimSpace(dateStr))
}

// the main function is the entry point of the go program, there can only be one
// the function name must be main
// the return type is void, it doesn't return anything
func main() {
	startTime := time.Now()
	// colly.NewCollector creates a new collector, it takes a variadic parameter
	// that is a list of options, in this case we are allowing the collector to
	// only visit links in the domain www.quepensaschacabuco.com
	c := colly.NewCollector(
		colly.AllowedDomains("www.quepensaschacabuco.com"),
		colly.Async(true),
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 5,
	})

	limiter := rate.NewLimiter(rate.Every(100*time.Millisecond), 1)

	// in go you can declare multiple variables in a single line using parentheses
	var (
		links    []string
		articles []Article
		// a mutex is a mutual exclusion lock, it is used to prevent multiple goroutines
		// from accessing the same resource at the same time, causing race conditions
		// a race condition is when two or more routines (in this case, goroutines)
		// try to access the same variable at the same time, causing a conflict
		mu sync.Mutex
		// a wait group is a counter that is used to wait for a group of goroutines to finish
		// in this case, we use it to wait for all the links to be processed
		wg        sync.WaitGroup
		pageCount int = 0
		linkCount int = 0
	)

	fmt.Println("Starting...")

	// in go, the & symbol is used to get the address of a variable
	// if we pass the variables without the &, we would be passing a copy of the variables
	// and the changes would not be reflected in the original variables
	setupCollectors(c, &links, &articles, &mu, &wg, &pageCount, &linkCount, limiter)

	if err := c.Visit("https://www.quepensaschacabuco.com/entradas/0/"); err != nil {
		log.Fatal(err)
	}

	c.Wait()
	// wait for all the links to be processed
	wg.Wait()

	printResults(articles, startTime, linkCount)
}

// this function sets up the collectors, it takes the collectors and the variables as pointers
// the pointers are necessary because we pass the address of the variables to the function
func setupCollectors(c *colly.Collector, links *[]string, articles *[]Article, mu *sync.Mutex, wg *sync.WaitGroup, pageCount *int, linkCount *int, limiter *rate.Limiter) {

	fmt.Println("Collectors...")

	setupMainCollector(c, links, pageCount, ".categoria_8")
	contentCollector := setupContentCollector(c, articles, mu, linkCount, limiter)
	setupOnScrapedCallback(c, contentCollector, links, wg)
}

// this function sets up the main collector
// it is responsible for visiting the pages and getting the links to the ScrapeArticles
func setupMainCollector(c *colly.Collector, links *[]string, pageCount *int, additionalClass string) {
	// this callback is triggered when the collector finds an HTML element with the attribute data-link
	// the data-link attribute is set by the website in the links to the articles
	fmt.Println("Main collector...")

	class := " .noticia1" + additionalClass

	fmt.Println("Main collector class " + class)

	c.OnHTML("[data-link]"+class, func(e *colly.HTMLElement) {
		parent := e.DOM.Parent()
		// e.DOM is the HTML element, we use it to get the value of the data-link attribute
		if link := parent.AttrOr("data-link", ""); link != "" {
			// if we don't use the * the new links will not be added to the original array
			*links = append(*links, link)
		}
	})

	c.OnScraped(func(r *colly.Response) {
		if len(*links) == 0 {
			log.Println("Error: No elements found with the specified selector.")
		}
	})

	c.OnHTML(".pagination a", func(e *colly.HTMLElement) {
		if e.Text == "Siguiente" {
			nextPage := e.Attr("href")
			*pageCount++
			e.Request.Visit(nextPage)
		}
	})
}

// this function sets up the content collector
// it is responsible for visiting the pages and getting the articles
// it returns the contentCollector so we can use it in the OnScraped callback
func setupContentCollector(c *colly.Collector, articles *[]Article, mu *sync.Mutex, linkCount *int, limiter *rate.Limiter) *colly.Collector {
	contentCollector := c.Clone()

	contentCollector.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting link: %s", r.URL.String())
	})

	contentCollector.OnHTML(".noticia-detalle", func(e *colly.HTMLElement) {
		article := parseArticle(e)
		// if the article is not nil, we add it to the articles array
		if article != nil {
			// we lock the mutex to prevent multiple goroutines from accessing the articles array at the same time
			mu.Lock()
			*articles = append(*articles, *article)
			*linkCount++ // Increment the link count
			// we unlock the mutex
			mu.Unlock()
		}
	})

	// this callback is triggered when the collector encounters an error
	// it takes the response and the error
	contentCollector.OnError(func(r *colly.Response, err error) {
		// if the status code is 429 (too many requests) or greater than or equal to 500 (server error)
		// we retry the request
		// we use the context to pass the retries to the next request
		// the context is a data structure that contains the request information
		// the GetAny function is used to get the value of the retries from the context
		// the Put function is used to add the retries to the context
		if r.StatusCode == 429 || r.StatusCode >= 500 {
			retries := r.Request.Ctx.GetAny("retries")
			if retries == nil {
				retries = 0
			}
			if retries.(int) < 3 {
				r.Request.Ctx.Put("retries", retries.(int)+1)
				limiter.Wait(context.Background())
				r.Request.Retry()
			}
		}
	})

	return contentCollector
}

// this function parses the article
// it takes the HTML element and returns an Article pointer
func parseArticle(e *colly.HTMLElement) *Article {
	info := e.ChildText(".noticia-detalle-info")
	title := e.ChildText(".titulo2 ")
	category := e.ChildText(".titulo")
	classes := e.DOM.AttrOr("class", "")
	// the regexp is a regular expression that is used to match a pattern in a string
	// the MustCompile function compiles the regexp and returns a regexp.Regexp pointer
	// the FindStringSubmatch function finds the first match of the regexp in the string
	// and returns a slice of strings containing the matched text

	lat, lng := Geocode("Chacabuco")
	fmt.Printf("LAT Y LNG -> %f %f\n", lat, lng)

	re := regexp.MustCompile(`categoria_(\d+)`)
	matches := re.FindStringSubmatch(classes)
	if len(matches) < 2 {
		log.Printf("Error: the categoryId was not found for the article: %s", title)
		return nil
	}
	// strconv.Atoi is a function that converts a string to an integer
	categoryId, err := strconv.Atoi(matches[1])
	if err != nil {
		log.Printf("Error parsing categoryId: %v", err)
		return nil
	}

	body := strings.Join(e.ChildTexts("p"), "\n\n")

	t, err := parseSpanishDate(info)
	if err != nil {
		log.Printf("Error parsing date: %v", err)
		return nil
	}

	log.Printf("Parsed article: %s", e.Request.URL.String())

	// we append a & to the article so we can add it to the articles array,
	// otherwise we would be adding a copy of the article to the array
	// there cant be a space between the article and the opening curly bracket, it would be an error
	return &Article{
		Title:      title,
		Date:       t.Format("2006-01-02 15:04:05"),
		Category:   category,
		CategoryId: categoryId,
		Body:       body,
		Link:       e.Request.URL.String(),
	}
}

// this function sets up the OnScraped callback
// it is triggered when the collector has finished visiting all the links
// it takes the contentCollector so we can use it to visit the links of the articles
// and the links and wg so we can add the links to the list and wait for the goroutines to finish
func setupOnScrapedCallback(c *colly.Collector, contentCollector *colly.Collector, links *[]string, wg *sync.WaitGroup) {
	c.OnScraped(func(r *colly.Response) {
		// we iterate over the links and add them to the wait group
		// the first parameter is the index, the second is the value
		for _, link := range *links {
			// we add 1 to the wait group, because we are adding a new link to be processed
			wg.Add(1)
			// we create a new goroutine that visits the link
			// a goroutine is a lightweight thread managed by the go runtime, it is not a system thread
			// it is cheaper in terms of resources and we can create a lot of them
			// they are not as powerful as system threads, but they are managed by the go runtime
			// and we can create a lot of them (thousands)
			// when we use the go keyword, it creates a new goroutine and the function is executed in that goroutine
			// the function is executed in the same address space as the main program, so we can share memory
			// the go keyword is like the await keyword in javascript, it returns immediately and the function is executed in a new goroutine
			go func(url string) {
				// the defer keyword is used to delay the execution of a function until the parent function returns
				// in this case, we use it to tell the wait group that we finished processing the link
				// if we don't do this, the program will exit before the goroutines finish processing the links
				defer wg.Done()
				if err := contentCollector.Visit(url); err != nil {
					log.Printf("Error visiting %s: %v", url, err)
				}
			}(link)
		}
		*links = []string{}
	})
}

// this function prints the results
// it takes the articles, the start time and the links
func printResults(articles []Article, startTime time.Time, linkCount int) {
	// js equivalent of json.MarshalIndent is JSON.stringify(articles, null, 2);
	// the first parameter is the data to marshal, the second is the prefix and the third is the indent
	json, err := json.MarshalIndent(articles, "", "  ")
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	// os.WriteFile is a function that writes the json to a file
	// the first parameter is the file name, the second is the data to write, the third is the permission
	// the permission is 0644, which means that the file can be read and written by the user and the group, but not by others
	err = os.WriteFile("articles.json", json, 0644)
	if err != nil {
		log.Fatalf("Error writing JSON file: %v", err)
	}
	log.Println("Results saved to articles.json")

	fmt.Println(string(json))

	fmt.Printf("Execution time: %s\nTotal articles scraped: %d\n",
		time.Since(startTime), linkCount)
}
