package knoelab.classification;

import java.io.File;
import java.io.PrintWriter;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.OWLFunctionalSyntaxOntologyFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

public class SampleClassifier {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("OWL file is required as input");
			System.exit(-1);
		}
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();		
		File ontFile = new File(args[0]);
		IRI documentIRI = IRI.create(ontFile);
		System.out.println("Loading the given Ontology...");
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		
		Normalizer normalizer = new Normalizer(manager,ontology);
		System.out.println("Normalizing the given Ontology....");
		OWLOntology normalizedOntology = normalizer.Normalize();
		System.out.println("Normalization complete");
		OWLFunctionalSyntaxOntologyFormat functionalSyntaxFormat = new OWLFunctionalSyntaxOntologyFormat();
		File outputFile = new File("Norm-func-" + ontFile.getName());
		manager.saveOntology(normalizedOntology, functionalSyntaxFormat, IRI.create(outputFile));
		System.out.println("Ontology serialized");
		
		System.out.println("Classifying using Pellet....");
		
		GregorianCalendar startTime = new GregorianCalendar();
		PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
		Set<OWLClass> classes = ontology.getClassesInSignature();
		
		File file = new File("classifier-output-" + ontFile.getName().split("\\.")[0] + ".txt");
		PrintWriter writer = new PrintWriter(file);
		
		for (OWLClass cl : classes) {
			Set<OWLClass> reasonerSuperclasses = pelletReasoner.getSuperClasses(cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperclasses.add(cl);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator = pelletReasoner.getEquivalentClasses(cl).iterator();
			while(iterator.hasNext())
				reasonerSuperclasses.add(iterator.next());
			
			writer.println("Class name: " + cl.toString());
			writer.println("Set of superclasses: ");
			for(OWLClass scl : reasonerSuperclasses)
				writer.println(scl);
			writer.println();
		}
		GregorianCalendar endTime = new GregorianCalendar();
		double diff = (endTime.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long mins = (long)diff/60;
		double secs = diff%60;
		System.out.println("Pellet Classification time: " + mins + "mins " + secs + "secs");
		writer.flush();
		writer.close();
	}

}
