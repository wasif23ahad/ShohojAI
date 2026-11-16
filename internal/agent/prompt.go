package agent

import (
	"fmt"
	"strings"
)

// FallbackBanglaReply is sent when LLM inference fails unexpectedly.
const FallbackBanglaReply = "দুঃখিত স্যার/ম্যাডাম, এই মুহূর্তে আমাদের সিস্টেমে সাময়িক যান্ত্রিক ত্রুটি হচ্ছে। অনুগ্রহ করে কিছুক্ষণ পর আবার চেষ্টা করুন অথবা আমাদের কাস্টমার কেয়ার প্রতিনিধির জন্য অপেক্ষা করুন। ধন্যবাদ!"

// EscalationConfirmationBangla is returned when a conversation is escalated to a human agent.
const EscalationConfirmationBangla = "জি স্যার/ম্যাডাম, আপনার বিষয়টি আমাদের কাস্টমার সাপোর্ট টিমের কাছে পাঠানো হয়েছে। আমাদের একজন প্রতিনিধি খুব শীঘ্রই আপনার সাথে যোগাযোগ করবেন। ধন্যবাদ!"

// BaseSystemPrompt defines the Bangladeshi business Bangla persona, deterministic tool rules, and few-shot examples.
const BaseSystemPrompt = `আপনি একজন অভিজ্ঞ, ভদ্র এবং আন্তরিক বাংলাদেশী ই-কমার্স কাস্টমার সাপোর্ট ও সেলস অ্যাসিস্ট্যান্ট (AI Agent)।

## ১. ভাষা ও কথা বলার ধরন (Linguistic Guidelines)
- গ্রাহক বাংলা (Bangla script), ইংরেজি (English), অথবা বাংলিশে (Romanized Bengali / Banglish, যেমন: "vai", "dam koto", "ei product ta ache?", "dhakar vitore delivery charge koto?") মেসেজ দিতে পারেন।
- আপনাকে সর্বদা প্রমিত ও স্বাভাবিক **বাংলাদেশী ব্যবসায়িক চলিত বাংলায় (natural Bangladeshi colloquial business Bangla)** উত্তর দিতে হবে।
- সর্বদা সম্মানসূচক সর্বনাম **"আপনি / আপনার"** ব্যবহার করবেন। কখনোই "তুমি" বা "তুই" ব্যবহার করবেন না।
- ই-কমার্সে প্রচলিত স্বাভাবিক শব্দগুলো বাংলায় বা ইংরেজিতে অপরিবর্তিত রাখবেন: যেমন *ডেলিভারি চার্জ*, *ক্যাশ অন ডেলিভারি*, *সাইজ*, *অর্ডার কনফার্ম*, *ইনবক্স*, *স্টকে আছে*।

## ২. কঠোর ব্যবসায়িক ও টুল ব্যবহারের নিয়ম (Deterministic Commerce Rules)
- **কখনোই অনুমান করে দাম, স্টক বা ডেলিভারি চার্জ বলবেন না।**
- পণ্যের খোঁজ করতে অবশ্যই ` + "`search_products`" + ` টুল কল করবেন।
- পণ্যের বর্তমান স্টক ও দাম জানতে অবশ্যই ` + "`check_stock`" + ` টুল কল করবেন।
- ডেলিভারি চার্জ ও সময় জানতে অবশ্যই ` + "`calculate_delivery_fee`" + ` টুল কল করবেন (ঢাকার ভেতরে ৭০ টাকা, ঢাকার পার্শ্ববর্তী গাজীপুর/নারায়ণগঞ্জ/সাভার/কেরানীগঞ্জ ১০০ টাকা, ঢাকার বাইরে ১৩০ টাকা)।
- অর্ডার সম্পন্ন করার আগে গ্রাহকের **নাম (Name)**, **পূর্ণ ঠিকানা (Full Address)**, **জেলা/এরিয়া (District)** এবং **১১ ডিজিটের বাংলাদেশী মোবাইল নম্বর (01XXXXXXXXX)** নিশ্চিত হয়ে ` + "`create_order`" + ` টুল কল করবেন।
- গ্রাহক যদি অভিযোগ করেন, রিফান্ড চান, রাগান্বিত হন বা মানুষের সাথে কথা বলতে চান, তবে সাথে সাথে ` + "`escalate_to_human`" + ` টুল কল করবেন।

## ৩. আদর্শ কথোপকথনের উদাহরণ (Few-Shot Examples)
- **গ্রাহক (Banglish):** vai ei black hoodie ta XXL size hobe? dhakar vitore delivery charge koto?
- **আপনি (Bangla):** জি স্যার/ম্যাডাম, আমাদের এই ব্ল্যাক হুডিটির XXL সাইজ বর্তমানে স্টকে আছে। ঢাকার ভেতরে ডেলিভারি চার্জ ৭০ টাকা (ক্যাশ অন ডেলিভারি)। আপনি কি অর্ডারটি কনফার্ম করতে চান?

- **গ্রাহক (English + Banglish):** order korbo kivabe? advance lagbe naki?
- **আপনি (Bangla):** অর্ডার কনফার্ম করার জন্য অনুগ্রহ করে আপনার নাম, পূর্ণ ঠিকানা এবং মোবাইল নম্বরটি দিন। কোনো অগ্রিম পেমেন্ট লাগবে না—পণ্য হাতে পেয়ে ক্যাশ অন ডেলিভারিতে পেমেন্ট করতে পারবেন।`

// BuildSystemPrompt constructs the system prompt for a specific customer turn.
func BuildSystemPrompt(senderID string) string {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return BaseSystemPrompt
	}
	return fmt.Sprintf("%s\n\n## ৪. বর্তমান সেশন তথ্য\n- Customer Sender ID: `%s`", BaseSystemPrompt, senderID)
}

// banglishLexicon maps common phonetic Banglish words to normalized English + Bangla search terms
// so hybrid pgvector + pg_trgm product search achieves high recall on code-mixed queries.
var banglishLexicon = map[string]string{
	"kalo":     "black কালো",
	"shada":    "white সাদা",
	"sada":     "white সাদা",
	"lal":      "red লাল",
	"nil":      "blue নীল",
	"neel":     "blue নীল",
	"sobuj":    "green সবুজ",
	"shobuj":   "green সবুজ",
	"holud":    "yellow হলুদ",
	"hudi":     "hoodie হুডি",
	"hoodie":   "hoodie হুডি",
	"hoody":    "hoodie হুডি",
	"panjabi":  "panjabi পাঞ্জাবি",
	"punjabi":  "panjabi পাঞ্জাবি",
	"tshirt":   "t-shirt টি-শার্ট",
	"ganji":    "t-shirt টি-শার্ট গেঞ্জি",
	"genji":    "t-shirt টি-শার্ট গেঞ্জি",
	"shari":    "saree শাড়ি",
	"saree":    "saree শাড়ি",
	"sari":     "saree শাড়ি",
	"borka":    "borka বোরকা",
	"hijab":    "hijab হিজাব",
	"juta":     "shoes জুতা",
	"ghori":    "watch ঘড়ি",
	"bag":      "bag ব্যাগ",
	"chinos":   "chinos প্যান্ট",
	"pant":     "pant প্যান্ট",
	"shirt":    "shirt শার্ট",
	"winter":   "winter শীতের",
	"shiter":   "winter শীতের",
	"cotton":   "cotton সুতি কটন",
	"shuti":    "cotton সুতি কটন",
	"suti":     "cotton সুতি কটন",
}

// banglishStopwords are conversational filler words stripped when building a clean product search query.
var banglishStopwords = map[string]bool{
	"vai":    true,
	"bhai":   true,
	"apu":    true,
	"ei":     true,
	"ta":     true,
	"ti":     true,
	"ki":     true,
	"ache":   true,
	"ase":    true,
	"hobe":   true,
	"koto":   true,
	"dam":    true,
	"daam":   true,
	"price":  true,
	"er":     true,
	"lagbe":  true,
	"nibo":   true,
	"korbo":  true,
	"please": true,
	"plz":    true,
}

// NormalizeBanglishQuery normalizes a phonetic Banglish / code-mixed customer product query
// into clean English + Bangla search terms for hybrid database retrieval.
func NormalizeBanglishQuery(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	cleaned := strings.NewReplacer(
		"?", " ",
		"!", " ",
		",", " ",
		".", " ",
		"/", " ",
	).Replace(raw)

	tokens := strings.Fields(cleaned)
	var out []string
	seen := make(map[string]bool)

	addToken := func(t string) {
		for _, part := range strings.Fields(t) {
			lower := strings.ToLower(part)
			if !seen[lower] {
				seen[lower] = true
				out = append(out, part)
			}
		}
	}

	for _, tok := range tokens {
		lower := strings.ToLower(tok)
		if banglishStopwords[lower] {
			continue
		}
		if expanded, ok := banglishLexicon[lower]; ok {
			addToken(expanded)
			continue
		}
		addToken(tok)
	}

	if len(out) == 0 {
		return raw
	}
	return strings.Join(out, " ")
}
