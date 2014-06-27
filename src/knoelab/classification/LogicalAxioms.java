package knoelab.classification;

import java.io.File;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.RDFXMLOntologyFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

public class LogicalAxioms {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		File ontFile = new File(args[0]);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		Normalizer normalizer = new Normalizer(manager, ontology);
	    OWLOntology normalizedOntology = normalizer.Normalize();
	    RDFXMLOntologyFormat owlxmlFormat = new RDFXMLOntologyFormat();
	    File file = new File("Norm-" + ontFile.getName());
	    manager.saveOntology(normalizedOntology, owlxmlFormat, IRI.create(file));
		System.out.println("No of concepts, logical axioms before normalization: " + 
	    		ontology.getClassesInSignature().size() + ", " + ontology.getAxiomCount());
	    System.out.println("No of concepts, logical axioms after normalization: " + 
	    		normalizedOntology.getClassesInSignature().size() + ", " + normalizedOntology.getAxiomCount());
	}
}
