import unittest
from airflow.models import DagBag
import os


class TestDagIntegrity(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag(dag_folder=os.getcwd() + '/dags', include_examples=False)

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_print_context_present(self):
        for dag_id, dag in self.dagbag.dags.items():
            desc = dag.default_args.get('description', '')
            msg = 'Description not set for DAG {id}'.format(id=dag_id)
            if desc != '':
                self.assertIn('A simple test DAG {}'.format(dag_id), desc, msg)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)
