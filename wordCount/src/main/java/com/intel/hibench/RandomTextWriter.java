package com.intel.hibench;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by peilunzh on 5/15/2015.
 * This mapreduce generates a random text for word count
 */
public class RandomTextWriter extends AbstractMapReduce {

    private static final Logger LOG = LoggerFactory.getLogger(RandomTextWriter.class);

    @UseDataSet("benchData")
    private Table benchData;
    private static long numBytesToWrite = 100 * 1024 * 1024;
    private static String BENCH_SIZE = "size";

    static final byte[] ONE = {'1'};
    static final byte[] THREE = {'3'};

    @Override
    public void configure() {
        setOutputDataset("lines");
        setMapperResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {

        Job job = context.getHadoopJob();
        job.setInputFormatClass(RandomInputFormat.class);
        job.setMapperClass(Generator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);


        String sizeStr = context.getRuntimeArguments().get("size");
        if (sizeStr != null) {
            LOG.info("size we get in config is : " + sizeStr);
            long totalBytes = Long.valueOf(sizeStr) * 1024 * 1024;
            job.getConfiguration().setLong(BENCH_SIZE, totalBytes);
            benchData.put(new Put(ONE, THREE, totalBytes));
        }

    }

    public static class Generator extends Mapper<Text, Text, Text, Text> {


        private Random random = new Random();

        @Override
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {

            long totalBytes = context.getConfiguration().getLong(BENCH_SIZE, numBytesToWrite);
            LOG.info("Bytes we get in map is: " + String.valueOf(totalBytes));

            while (totalBytes > 0) {
                // Generate the key/value
                int noWordsKey = 10;
                int noWordsValue = 100;
                Text keyWords = generateSentence(noWordsKey);
                Text valueWords = generateSentence(noWordsValue);

                // Write the sentence
                context.write(keyWords, valueWords);

                totalBytes -= (keyWords.getLength() + valueWords.getLength());
            }
        }

        private Text generateSentence(int noWords) {
            StringBuffer sentence = new StringBuffer();
            String space = " ";
            for (int i = 0; i < noWords; ++i) {
                sentence.append(words[random.nextInt(words.length)]);
                sentence.append(space);
            }
            return new Text(sentence.toString());
        }
    }


    static class RandomInputFormat extends InputFormat<Text, Text> {

        /**
         * Generate the requested number of file splits, with the filename
         * set to the filename of the output file.
         */
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> result = new ArrayList<InputSplit>();
            Path outDir = FileOutputFormat.getOutputPath(job);
            int numSplits =
                    job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            for (int i = 0; i < numSplits; ++i) {
                result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1,
                        (String[]) null));
            }
            return result;
        }

        /**
         * Return a single record (filename, "") where the filename is taken from
         * the file split.
         */
        static class RandomRecordReader extends RecordReader<Text, Text> {
            Path name;
            Text key = null;
            Text value = new Text();

            public RandomRecordReader(Path p) {
                name = p;
            }

            public void initialize(InputSplit split,
                                   TaskAttemptContext context)
                    throws IOException, InterruptedException {

            }

            public boolean nextKeyValue() {
                if (name != null) {
                    key = new Text();
                    key.set(name.getName());
                    name = null;
                    return true;
                }
                return false;
            }

            public Text getCurrentKey() {
                return key;
            }

            public Text getCurrentValue() {
                return value;
            }

            public void close() {
            }

            public float getProgress() {
                return 0.0f;
            }
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) throws IOException, InterruptedException {
            return new RandomRecordReader(((FileSplit) split).getPath());
        }
    }


    private static String[] words = {
            "diurnalness", "Homoiousian",
            "spiranthic", "tetragynian",
            "silverhead", "ungreat",
            "lithograph", "exploiter",
            "physiologian", "by",
            "hellbender", "Filipendula",
            "undeterring", "antiscolic",
            "pentagamist", "hypoid",
            "cacuminal", "sertularian",
            "schoolmasterism", "nonuple",
            "gallybeggar", "phytonic",
            "swearingly", "nebular",
            "Confervales", "thermochemically",
            "characinoid", "cocksuredom",
            "fallacious", "feasibleness",
            "debromination", "playfellowship",
            "tramplike", "testa",
            "participatingly", "unaccessible",
            "bromate", "experientialist",
            "roughcast", "docimastical",
            "choralcelo", "blightbird",
            "peptonate", "sombreroed",
            "unschematized", "antiabolitionist",
            "besagne", "mastication",
            "bromic", "sviatonosite",
            "cattimandoo", "metaphrastical",
            "endotheliomyoma", "hysterolysis",
            "unfulminated", "Hester",
            "oblongly", "blurredness",
            "authorling", "chasmy",
            "Scorpaenidae", "toxihaemia",
            "Dictograph", "Quakerishly",
            "deaf", "timbermonger",
            "strammel", "Thraupidae",
            "seditious", "plerome",
            "Arneb", "eristically",
            "serpentinic", "glaumrie",
            "socioromantic", "apocalypst",
            "tartrous", "Bassaris",
            "angiolymphoma", "horsefly",
            "kenno", "astronomize",
            "euphemious", "arsenide",
            "untongued", "parabolicness",
            "uvanite", "helpless",
            "gemmeous", "stormy",
            "templar", "erythrodextrin",
            "comism", "interfraternal",
            "preparative", "parastas",
            "frontoorbital", "Ophiosaurus",
            "diopside", "serosanguineous",
            "ununiformly", "karyological",
            "collegian", "allotropic",
            "depravity", "amylogenesis",
            "reformatory", "epidymides",
            "pleurotropous", "trillium",
            "dastardliness", "coadvice",
            "embryotic", "benthonic",
            "pomiferous", "figureheadship",
            "Megaluridae", "Harpa",
            "frenal", "commotion",
            "abthainry", "cobeliever",
            "manilla", "spiciferous",
            "nativeness", "obispo",
            "monilioid", "biopsic",
            "valvula", "enterostomy",
            "planosubulate", "pterostigma",
            "lifter", "triradiated",
            "venialness", "tum",
            "archistome", "tautness",
            "unswanlike", "antivenin",
            "Lentibulariaceae", "Triphora",
            "angiopathy", "anta",
            "Dawsonia", "becomma",
            "Yannigan", "winterproof",
            "antalgol", "harr",
            "underogating", "ineunt",
            "cornberry", "flippantness",
            "scyphostoma", "approbation",
            "Ghent", "Macraucheniidae",
            "scabbiness", "unanatomized",
            "photoelasticity", "eurythermal",
            "enation", "prepavement",
            "flushgate", "subsequentially",
            "Edo", "antihero",
            "Isokontae", "unforkedness",
            "porriginous", "daytime",
            "nonexecutive", "trisilicic",
            "morphiomania", "paranephros",
            "botchedly", "impugnation",
            "Dodecatheon", "obolus",
            "unburnt", "provedore",
            "Aktistetae", "superindifference",
            "Alethea", "Joachimite",
            "cyanophilous", "chorograph",
            "brooky", "figured",
            "periclitation", "quintette",
            "hondo", "ornithodelphous",
            "unefficient", "pondside",
            "bogydom", "laurinoxylon",
            "Shiah", "unharmed",
            "cartful", "noncrystallized",
            "abusiveness", "cromlech",
            "japanned", "rizzomed",
            "underskin", "adscendent",
            "allectory", "gelatinousness",
            "volcano", "uncompromisingly",
            "cubit", "idiotize",
            "unfurbelowed", "undinted",
            "magnetooptics", "Savitar",
            "diwata", "ramosopalmate",
            "Pishquow", "tomorn",
            "apopenptic", "Haversian",
            "Hysterocarpus", "ten",
            "outhue", "Bertat",
            "mechanist", "asparaginic",
            "velaric", "tonsure",
            "bubble", "Pyrales",
            "regardful", "glyphography",
            "calabazilla", "shellworker",
            "stradametrical", "havoc",
            "theologicopolitical", "sawdust",
            "diatomaceous", "jajman",
            "temporomastoid", "Serrifera",
            "Ochnaceae", "aspersor",
            "trailmaking", "Bishareen",
            "digitule", "octogynous",
            "epididymitis", "smokefarthings",
            "bacillite", "overcrown",
            "mangonism", "sirrah",
            "undecorated", "psychofugal",
            "bismuthiferous", "rechar",
            "Lemuridae", "frameable",
            "thiodiazole", "Scanic",
            "sportswomanship", "interruptedness",
            "admissory", "osteopaedion",
            "tingly", "tomorrowness",
            "ethnocracy", "trabecular",
            "vitally", "fossilism",
            "adz", "metopon",
            "prefatorial", "expiscate",
            "diathermacy", "chronist",
            "nigh", "generalizable",
            "hysterogen", "aurothiosulphuric",
            "whitlowwort", "downthrust",
            "Protestantize", "monander",
            "Itea", "chronographic",
            "silicize", "Dunlop",
            "eer", "componental",
            "spot", "pamphlet",
            "antineuritic", "paradisean",
            "interruptor", "debellator",
            "overcultured", "Florissant",
            "hyocholic", "pneumatotherapy",
            "tailoress", "rave",
            "unpeople", "Sebastian",
            "thermanesthesia", "Coniferae",
            "swacking", "posterishness",
            "ethmopalatal", "whittle",
            "analgize", "scabbardless",
            "naught", "symbiogenetically",
            "trip", "parodist",
            "columniform", "trunnel",
            "yawler", "goodwill",
            "pseudohalogen", "swangy",
            "cervisial", "mediateness",
            "genii", "imprescribable",
            "pony", "consumptional",
            "carposporangial", "poleax",
            "bestill", "subfebrile",
            "sapphiric", "arrowworm",
            "qualminess", "ultraobscure",
            "thorite", "Fouquieria",
            "Bermudian", "prescriber",
            "elemicin", "warlike",
            "semiangle", "rotular",
            "misthread", "returnability",
            "seraphism", "precostal",
            "quarried", "Babylonism",
            "sangaree", "seelful",
            "placatory", "pachydermous",
            "bozal", "galbulus",
            "spermaphyte", "cumbrousness",
            "pope", "signifier",
            "Endomycetaceae", "shallowish",
            "sequacity", "periarthritis",
            "bathysphere", "pentosuria",
            "Dadaism", "spookdom",
            "Consolamentum", "afterpressure",
            "mutter", "louse",
            "ovoviviparous", "corbel",
            "metastoma", "biventer",
            "Hydrangea", "hogmace",
            "seizing", "nonsuppressed",
            "oratorize", "uncarefully",
            "benzothiofuran", "penult",
            "balanocele", "macropterous",
            "dishpan", "marten",
            "absvolt", "jirble",
            "parmelioid", "airfreighter",
            "acocotl", "archesporial",
            "hypoplastral", "preoral",
            "quailberry", "cinque",
            "terrestrially", "stroking",
            "limpet", "moodishness",
            "canicule", "archididascalian",
            "pompiloid", "overstaid",
            "introducer", "Italical",
            "Christianopaganism", "prescriptible",
            "subofficer", "danseuse",
            "cloy", "saguran",
            "frictionlessly", "deindividualization",
            "Bulanda", "ventricous",
            "subfoliar", "basto",
            "scapuloradial", "suspend",
            "stiffish", "Sphenodontidae",
            "eternal", "verbid",
            "mammonish", "upcushion",
            "barkometer", "concretion",
            "preagitate", "incomprehensible",
            "tristich", "visceral",
            "hemimelus", "patroller",
            "stentorophonic", "pinulus",
            "kerykeion", "brutism",
            "monstership", "merciful",
            "overinstruct", "defensibly",
            "bettermost", "splenauxe",
            "Mormyrus", "unreprimanded",
            "taver", "ell",
            "proacquittal", "infestation",
            "overwoven", "Lincolnlike",
            "chacona", "Tamil",
            "classificational", "lebensraum",
            "reeveland", "intuition",
            "Whilkut", "focaloid",
            "Eleusinian", "micromembrane",
            "byroad", "nonrepetition",
            "bacterioblast", "brag",
            "ribaldrous", "phytoma",
            "counteralliance", "pelvimetry",
            "pelf", "relaster",
            "thermoresistant", "aneurism",
            "molossic", "euphonym",
            "upswell", "ladhood",
            "phallaceous", "inertly",
            "gunshop", "stereotypography",
            "laryngic", "refasten",
            "twinling", "oflete",
            "hepatorrhaphy", "electrotechnics",
            "cockal", "guitarist",
            "topsail", "Cimmerianism",
            "larklike", "Llandovery",
            "pyrocatechol", "immatchable",
            "chooser", "metrocratic",
            "craglike", "quadrennial",
            "nonpoisonous", "undercolored",
            "knob", "ultratense",
            "balladmonger", "slait",
            "sialadenitis", "bucketer",
            "magnificently", "unstipulated",
            "unscourged", "unsupercilious",
            "packsack", "pansophism",
            "soorkee", "percent",
            "subirrigate", "champer",
            "metapolitics", "spherulitic",
            "involatile", "metaphonical",
            "stachyuraceous", "speckedness",
            "bespin", "proboscidiform",
            "gul", "squit",
            "yeelaman", "peristeropode",
            "opacousness", "shibuichi",
            "retinize", "yote",
            "misexposition", "devilwise",
            "pumpkinification", "vinny",
            "bonze", "glossing",
            "decardinalize", "transcortical",
            "serphoid", "deepmost",
            "guanajuatite", "wemless",
            "arval", "lammy",
            "Effie", "Saponaria",
            "tetrahedral", "prolificy",
            "excerpt", "dunkadoo",
            "Spencerism", "insatiately",
            "Gilaki", "oratorship",
            "arduousness", "unbashfulness",
            "Pithecolobium", "unisexuality",
            "veterinarian", "detractive",
            "liquidity", "acidophile",
            "proauction", "sural",
            "totaquina", "Vichyite",
            "uninhabitedness", "allegedly",
            "Gothish", "manny",
            "Inger", "flutist",
            "ticktick", "Ludgatian",
            "homotransplant", "orthopedical",
            "diminutively", "monogoneutic",
            "Kenipsim", "sarcologist",
            "drome", "stronghearted",
            "Fameuse", "Swaziland",
            "alen", "chilblain",
            "beatable", "agglomeratic",
            "constitutor", "tendomucoid",
            "porencephalous", "arteriasis",
            "boser", "tantivy",
            "rede", "lineamental",
            "uncontradictableness", "homeotypical",
            "masa", "folious",
            "dosseret", "neurodegenerative",
            "subtransverse", "Chiasmodontidae",
            "palaeotheriodont", "unstressedly",
            "chalcites", "piquantness",
            "lampyrine", "Aplacentalia",
            "projecting", "elastivity",
            "isopelletierin", "bladderwort",
            "strander", "almud",
            "iniquitously", "theologal",
            "bugre", "chargeably",
            "imperceptivity", "meriquinoidal",
            "mesophyte", "divinator",
            "perfunctory", "counterappellant",
            "synovial", "charioteer",
            "crystallographical", "comprovincial",
            "infrastapedial", "pleasurehood",
            "inventurous", "ultrasystematic",
            "subangulated", "supraoesophageal",
            "Vaishnavism", "transude",
            "chrysochrous", "ungrave",
            "reconciliable", "uninterpleaded",
            "erlking", "wherefrom",
            "aprosopia", "antiadiaphorist",
            "metoxazine", "incalculable",
            "umbellic", "predebit",
            "foursquare", "unimmortal",
            "nonmanufacture", "slangy",
            "predisputant", "familist",
            "preaffiliate", "friarhood",
            "corelysis", "zoonitic",
            "halloo", "paunchy",
            "neuromimesis", "aconitine",
            "hackneyed", "unfeeble",
            "cubby", "autoschediastical",
            "naprapath", "lyrebird",
            "inexistency", "leucophoenicite",
            "ferrogoslarite", "reperuse",
            "uncombable", "tambo",
            "propodiale", "diplomatize",
            "Russifier", "clanned",
            "corona", "michigan",
            "nonutilitarian", "transcorporeal",
            "bought", "Cercosporella",
            "stapedius", "glandularly",
            "pictorially", "weism",
            "disilane", "rainproof",
            "Caphtor", "scrubbed",
            "oinomancy", "pseudoxanthine",
            "nonlustrous", "redesertion",
            "Oryzorictinae", "gala",
            "Mycogone", "reappreciate",
            "cyanoguanidine", "seeingness",
            "breadwinner", "noreast",
            "furacious", "epauliere",
            "omniscribent", "Passiflorales",
            "uninductive", "inductivity",
            "Orbitolina", "Semecarpus",
            "migrainoid", "steprelationship",
            "phlogisticate", "mesymnion",
            "sloped", "edificator",
            "beneficent", "culm",
            "paleornithology", "unurban",
            "throbless", "amplexifoliate",
            "sesquiquintile", "sapience",
            "astucious", "dithery",
            "boor", "ambitus",
            "scotching", "uloid",
            "uncompromisingness", "hoove",
            "waird", "marshiness",
            "Jerusalem", "mericarp",
            "unevoked", "benzoperoxide",
            "outguess", "pyxie",
            "hymnic", "euphemize",
            "mendacity", "erythremia",
            "rosaniline", "unchatteled",
            "lienteria", "Bushongo",
            "dialoguer", "unrepealably",
            "rivethead", "antideflation",
            "vinegarish", "manganosiderite",
            "doubtingness", "ovopyriform",
            "Cephalodiscus", "Muscicapa",
            "Animalivora", "angina",
            "planispheric", "ipomoein",
            "cuproiodargyrite", "sandbox",
            "scrat", "Munnopsidae",
            "shola", "pentafid",
            "overstudiousness", "times",
            "nonprofession", "appetible",
            "valvulotomy", "goladar",
            "uniarticular", "oxyterpene",
            "unlapsing", "omega",
            "trophonema", "seminonflammable",
            "circumzenithal", "starer",
            "depthwise", "liberatress",
            "unleavened", "unrevolting",
            "groundneedle", "topline",
            "wandoo", "umangite",
            "ordinant", "unachievable",
            "oversand", "snare",
            "avengeful", "unexplicit",
            "mustafina", "sonable",
            "rehabilitative", "eulogization",
            "papery", "technopsychology",
            "impressor", "cresylite",
            "entame", "transudatory",
            "scotale", "pachydermatoid",
            "imaginary", "yeat",
            "slipped", "stewardship",
            "adatom", "cockstone",
            "skyshine", "heavenful",
            "comparability", "exprobratory",
            "dermorhynchous", "parquet",
            "cretaceous", "vesperal",
            "raphis", "undangered",
            "Glecoma", "engrain",
            "counteractively", "Zuludom",
            "orchiocatabasis", "Auriculariales",
            "warriorwise", "extraorganismal",
            "overbuilt", "alveolite",
            "tetchy", "terrificness",
            "widdle", "unpremonished",
            "rebilling", "sequestrum",
            "equiconvex", "heliocentricism",
            "catabaptist", "okonite",
            "propheticism", "helminthagogic",
            "calycular", "giantly",
            "wingable", "golem",
            "unprovided", "commandingness",
            "greave", "haply",
            "doina", "depressingly",
            "subdentate", "impairment",
            "decidable", "neurotrophic",
            "unpredict", "bicorporeal",
            "pendulant", "flatman",
            "intrabred", "toplike",
            "Prosobranchiata", "farrantly",
            "toxoplasmosis", "gorilloid",
            "dipsomaniacal", "aquiline",
            "atlantite", "ascitic",
            "perculsive", "prospectiveness",
            "saponaceous", "centrifugalization",
            "dinical", "infravaginal",
            "beadroll", "affaite",
            "Helvidian", "tickleproof",
            "abstractionism", "enhedge",
            "outwealth", "overcontribute",
            "coldfinch", "gymnastic",
            "Pincian", "Munychian",
            "codisjunct", "quad",
            "coracomandibular", "phoenicochroite",
            "amender", "selectivity",
            "putative", "semantician",
            "lophotrichic", "Spatangoidea",
            "saccharogenic", "inferent",
            "Triconodonta", "arrendation",
            "sheepskin", "taurocolla",
            "bunghole", "Machiavel",
            "triakistetrahedral", "dehairer",
            "prezygapophysial", "cylindric",
            "pneumonalgia", "sleigher",
            "emir", "Socraticism",
            "licitness", "massedly",
            "instructiveness", "sturdied",
            "redecrease", "starosta",
            "evictor", "orgiastic",
            "squdge", "meloplasty",
            "Tsonecan", "repealableness",
            "swoony", "myesthesia",
            "molecule", "autobiographist",
            "reciprocation", "refective",
            "unobservantness", "tricae",
            "ungouged", "floatability",
            "Mesua", "fetlocked",
            "chordacentrum", "sedentariness",
            "various", "laubanite",
            "nectopod", "zenick",
            "sequentially", "analgic",
            "biodynamics", "posttraumatic",
            "nummi", "pyroacetic",
            "bot", "redescend",
            "dispermy", "undiffusive",
            "circular", "trillion",
            "Uraniidae", "ploration",
            "discipular", "potentness",
            "sud", "Hu",
            "Eryon", "plugger",
            "subdrainage", "jharal",
            "abscission", "supermarket",
            "countergabion", "glacierist",
            "lithotresis", "minniebush",
            "zanyism", "eucalypteol",
            "sterilely", "unrealize",
            "unpatched", "hypochondriacism",
            "critically", "cheesecutter",
    };

}
