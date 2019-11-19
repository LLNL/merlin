
#include "conduit/conduit.hpp"
#include "conduit/conduit_relay.hpp"

using namespace conduit;
using namespace conduit::relay;

int main(int argc, char ** argv) {

   char * fname = argv[1];

   Node n_load;
   io::load(fname,"hdf5",n_load);
   n_load.info().print();
   n_load.schema().print();
   n_load["0/outputs/images/0"].print();
   double * abs_image = n_load["0/outputs/images/0/abs"].value();

   NodeIterator itr = n_load["0/outputs/images"].children();

   while (itr.has_next()) {
      Node & n = itr.next();
      std::string n_name = itr.name();
      std::cout << n_name << std::endl;
   }
}
