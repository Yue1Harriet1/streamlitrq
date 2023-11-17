from langchain.document_loaders import PyPDFLoader
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from . import settings

def read_pdf(filename:str, dir:str):
	return(PyPDFLoader(path+"//"+file).load_and_split())

def get_indexing(read_docs, EMBEDDINGS, path_index:str="./dbs/documentation/faiss_index"):
	openai.api_key = settings.OPENAI_API_KEY
	db = FAISS.from_documents(documents=read_docs, embedding=EMBEDDINGS)
	db.save_local(path_index)
	return(db)
	
def get_retriever(embeddings, path_index:str="./dbs/documentation/faiss_index"):
	vectorstore = FAISS.load_local(path_index, embeddings)
	return(vectorstore.as_retriever(search_type="similarity", search_kwargs={"k":2}))

def get_answer_to_document_query(query, filename:str, dir, retriever, llm=settings.LLM):
	qa = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=False)
	return(qa({"query": query})["result"])

def get_qa(query, filename:str, dir, path_index:str="./dbs/documentation/faiss_index", embeddings=settings.EMBEDDINGS, llm=settings.LLM):
	db = get_indexing(read_pdf(filename, dir), settings.EMBEDDINGS, path_index=path_index)
	retriever = get_retriever(embeddings, path_index=path_index)
	return(get_answer_to_document_query(query=query, filename=filename, dir=dir, retriever=retriever, llm=llm))
