from langchain.document_loaders import PyPDFLoader
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
import settings

def read_pdf(filename:str, dir:str):
	return(PyPDFLoader(path+"//"+file).load_and_split())

def get_indexing(read_docs, EMBEDDINGS):
	openai.api_key = settings.OPENAI_API_KEY
	db = FAISS.from_documents(documents=read_docs, embedding=EMBEDDINGS)
	db.save_local("./dbs/documentation/faiss_index")
	return(db)
	
def get_retriever(path_index, embeddings):
	vectorstore = FAISS.load_local(path_index, embeddings)
	return(vectorstore.as_retriever(search_type="similarity", search_kwargs={"k":2}))

def get_answer_to_document_query(query, filename:str, dir, retriever):
	qa = RetrievalQA.from_chain_type(llm=settings.LLM, chain_type="stuff", retriever=retriever, return_source_documents=False)
	return(qa({"query": query})["result"])

def get_qa(filename:str, dir):
	db = get_indexing(read_pdf(filename, dir))
